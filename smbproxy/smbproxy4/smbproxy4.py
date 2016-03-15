#!/usr/bin/env python2
# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import base64
from datetime import datetime
import json
import os
import struct
import traceback
import uuid

from twisted.internet import defer
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver
from twisted.python import log

from seekscale_commons.stream_stats import StreamStatsClient

from nmb.nmb_constants import *
from nmb.nmb_structs import DirectTCPSessionMessage, NMBError, NotConnectedError
from smb.smb_structs import SMBMessage, SMB2ProtocolHeaderError, ProtocolError
from smb.smb_constants import *
from smb.smb2_structs import SMB2Message
from smb.smb2_constants import *

from debug_interface import dump_debug_stats, get_debug_stats_struct
from fs_cache import FSCache
from fs_local_cache_client import FSLocalCacheClient
import logger
from statsd_logging import StatsdClient


class ProxyClientProtocol(protocol.Protocol):

    def connectionMade(self):
        # log.msg("Client: connected to peer")
        # cli_queue is where data from the client comes
        self.cli_queue = self.factory.cli_queue
        self.cli_queue.get().addCallback(self.serverDataReceived)

    def serverDataReceived(self, chunk):
        """
        This is where we react to data received from the client
        :param chunk:
        """
        if chunk is False:
            self.cli_queue = None
            # log.msg("Client: disconnecting from peer")
            self.factory.continueTrying = False
            self.transport.loseConnection()
        elif self.cli_queue:
            # log.msg("Client: writing %d bytes to peer" % len(chunk))
            self.transport.write(chunk)
            # Put back the callback for the next chunk of data
            self.cli_queue.get().addCallback(self.serverDataReceived)
        else:
            self.factory.cli_queue.put(chunk)

    def dataReceived(self, chunk):
        # log.msg("Client: %d bytes received from peer" % len(chunk))
        # In srv_queue, we put data that comes from the server
        self.factory.srv_queue.put(chunk)

    def connectionLost(self, why):
        if self.cli_queue:
            self.cli_queue = None
            # log.msg("Client: peer disconnected unexpectedly")

        self.factory.srv_queue.put(False)


# class ProxyClientFactory(protocol.ReconnectingClientFactory):
class ProxyClientFactory(protocol.ClientFactory):
    maxDelay = 10
    continueTrying = True
    protocol = ProxyClientProtocol

    def __init__(self, srv_queue, cli_queue):
        self.srv_queue = srv_queue
        self.cli_queue = cli_queue


class ProxyServerProtocol(protocol.Protocol):
    remote_host = None
    remote_port = None
    fscacheclient = None

    def __init__(self):
        self.log = None

        self.data_buf = ''
        self.response_data_buf = ''

        self.stats_client = StatsdClient.get()

        # Local connection state
        self.tree_connect_requests = {}
        self.connected_trees = {}
        self.file_open_requests = {}
        self.file_close_requests = {}
        self.open_files = {}
        self.session_latest_create_request_filename = None
        self.session_latest_tree_connect_path = None

        self.srv_queue = defer.DeferredQueue()
        self.cli_queue = defer.DeferredQueue()
        # Manually bind a reaction to the data that comes from the server
        self.srv_queue.get().addCallback(self.clientDataReceived)

        # The list of NMB packets from the client waiting to be processed
        self.client_pending_packets_queue = defer.DeferredQueue()
        self.client_pending_packets_queue_len = 0
        self.client_pending_packets_queue.get().addCallback(self.process_client_pending_packet)

        # The list of NMB packets from the server waiting to be processed
        self.server_pending_packets_queue = defer.DeferredQueue()
        self.server_pending_packets_queue_len = 0
        self.server_pending_packets_queue.get().addCallback(self.process_server_pending_packet)

        # Whether a shutdown has been requested
        self.shutdown_requested = False
        self.shutdown_deferred = None

        # Misc counters
        self.total_processed_client_packets = 0
        self.total_processed_server_packets = 0

    #
    # Connectivity functions
    #
    def connectionMade(self):
        self.factory.clients.append(self)

        # Setup the forward connection
        factory = ProxyClientFactory(self.srv_queue, self.cli_queue)
#        self.remote_host = self.transport.getPeer().host

        self.settings = self.factory.settings
        self.remote_host = self.settings.REMOTE_SAMBA_HOST
        self.remote_port = self.settings.REMOTE_SAMBA_PORT

        self.log = logger.logger.new(
            connection_id=str(uuid.uuid4()),
            peer=self.transport.getPeer().host
        )

        reactor.connectTCP(self.remote_host, self.remote_port, factory)

        # Initialize a FSCache for this client
        self.fscacheclient = self.factory.fscacheclient

    def clientDataReceived(self, chunk):
        # log.msg("Server: writing %d bytes to original client" % len(chunk))

        if chunk is False:
            # Disconnected from the backend SMB server. Disconnect the client.
            self.transport.loseConnection()
            return

        # Process the data
        self.feedDataResponse(chunk)

        # Write data to the client, and listen for the next chunk
        self.transport.write(chunk)
        self.srv_queue.get().addCallback(self.clientDataReceived)

    def shutdown(self):
        self.shutdown_requested = True
        self.shutdown_deferred = defer.Deferred()

        self.shutdown_deferred.addCallback(lambda x: self.transport.loseConnection())

        reactor.callLater(1, self.tryShutdown)

        return self.shutdown_deferred

    def dataReceived(self, chunk):
        # log.msg("Server: %d bytes received" % len(chunk))

        # Process the data
        self.feedData(chunk)

    def connectionLost(self, why):
        self.cli_queue.put(False)
        self.factory.clients.remove(self)

    def share_is_intercepted(self, share_unc):
        """
        :param_name share_unc:
        :return: True if we need to intercept calls on this share, False if we pass data unmodified to the smb server
        """
        # Ignore the admin shares
        if share_unc.endswith('IPC$') or share_unc.endswith('ADMIN$'):
            return False

        # Ignore the my_seekscale_data share
        if share_unc.lower().endswith('my_seekscale_data'):
            return False

        else:
            return True

    #
    # NMB handling functions
    #
    def feedData(self, data):
        """Decodes and processes forward-going data (from the client to the server)"""
        self.data_buf = self.data_buf + data

        while True:
            data_nmb = DirectTCPSessionMessage()
            length = data_nmb.decode(self.data_buf, 0)
            if length == 0:
                break
            elif length > 0:
                # Save the raw_chunk in the data_nmb packet, so we can pass it in a callback afterwards
                chunk = self.data_buf[:length]
                self.data_buf = self.data_buf[length:]
                data_nmb.raw_chunk = chunk

                self.client_pending_packets_queue.put((data_nmb, datetime.utcnow()))
                self.client_pending_packets_queue_len += 1
                # self._processNMBSessionPacket(self.data_nmb, self.onNMBSessionMessage)
            else:
                raise NMBError

        if self.client_pending_packets_queue_len > self.settings.PENDING_PACKETS_LEVEL_WARN:
            self.log.msg(
                "%d packets pending in client data queue !" % self.client_pending_packets_queue_len,
                level=logger.WARN
            )

    def feedDataResponse(self, data):
        """
        Decodes and processes backward-going data (from the server to the client)
        :param data:
        :return:
        """
        self.response_data_buf = self.response_data_buf + data

        offset = 0
        while True:
            response_data_nmb = DirectTCPSessionMessage()
            length = response_data_nmb.decode(self.response_data_buf, offset)
            if length == 0:
                break
            elif length > 0:
                # log.msg("Found response NMB packet of length %d" % length)
                offset += length
                self.server_pending_packets_queue.put((response_data_nmb, datetime.utcnow()))
                self.server_pending_packets_queue_len += 1
                # self._processNMBSessionPacket(self.response_data_nmb, self.onNMBSessionMessageResponse)
            else:
                raise NMBError

        if self.server_pending_packets_queue_len > self.settings.PENDING_PACKETS_LEVEL_WARN:
            self.log.msg(
                "%d packets pending in server data queue !" % self.server_pending_packets_queue_len,
                logger.WARN
            )

        if offset > 0:
            self.response_data_buf = self.response_data_buf[offset:]

    @defer.inlineCallbacks
    def process_client_pending_packet(self, arg):
        """Process a SMB packet coming from the client"""
        packet, packet_reception_time = arg
        chunk = packet.raw_chunk

        try:
            if packet.type == SESSION_MESSAGE:
                yield self.onNMBSessionMessage(packet)
            else:
                self.log.msg('Unrecognized NMB session type: 0x%02x' % packet.type, level=logger.WARN)
        except Exception:
            self.log.msg("Couldn't process client packet: %s" % traceback.format_exc(), level=logger.WARN)

        packet_processing_time = (datetime.utcnow() - packet_reception_time).total_seconds()*1000
        self.stats_client.incr('packet.inbound.count')
        self.stats_client.timing('packet.inbound.processing_time', int(packet_processing_time))

        # Pass the data to the real server
        self.cli_queue.put(chunk)

        # Mark the packet as processed, and schedule a callback for the next packet
        self.total_processed_client_packets += 1
        self.client_pending_packets_queue_len -= 1
        self.client_pending_packets_queue.get().addCallback(self.process_client_pending_packet)

    def process_server_pending_packet(self, arg):
        """Process a SMB packet coming from the server"""
        packet, packet_reception_time = arg
        try:
            self._processNMBSessionPacket(packet, self.onNMBSessionMessageResponse)
        except Exception:
            self.log.msg("Couldn't process server packet: %s" % traceback.format_exc(), level=logger.WARN)

        packet_processing_time = (datetime.utcnow() - packet_reception_time).total_seconds()*1000
        self.stats_client.incr('packet.outbound.count')
        self.stats_client.timing('packet.outbound.processing_time', int(packet_processing_time))

        self.total_processed_server_packets += 1
        self.server_pending_packets_queue_len -= 1
        self.server_pending_packets_queue.get().addCallback(self.process_server_pending_packet)

    def _processNMBSessionPacket(self, packet, callback):
        # log.msg('Got NMB packet. Passing to %s' % callback)
        if packet.type == SESSION_MESSAGE:
            # self.onNMBSessionMessage(packet.flags, packet.data)
            callback(packet.flags, packet.data)
        else:
            self.log.msg('Unrecognized NMB session type: 0x%02x' % packet.type, level=logger.WARN)

    def onNMBSessionMessage(self, message):
        is_using_smb2 = False
        flags = message.flags
        data = message.data

        # Dummy way to initiate the callback chain
        d = defer.succeed(None)

        def process_smb_message(msg):
            return self._updateState_SMB2(msg)

        while True:
            try:
                i, smb_message = self.peekSMBMessageType(data)
            except SMB2ProtocolHeaderError:
                is_using_smb2 = True
                i, smb_message = self.peekSMB2MessageType(data)

            next_message_offset = 0
            if is_using_smb2:
                next_message_offset = smb_message.next_command_offset

            if i > 0:
                if is_using_smb2:
                    # print 'Received SMB2 message "%s" (command:0x%04X flags:0x%04x)' % (SMB2_COMMAND_NAMES.get(self.smb_message.command, '<unknown>'), self.smb_message.command, self.smb_message.flags)
                    d.addCallback(lambda x: smb_message)
                    d.addCallback(process_smb_message)

                else:
                    # print 'Received SMB message "%s" (command:0x%2X flags:0x%02X flags2:0x%04X TID:%d UID:%d)' % (SMB_COMMAND_NAMES.get(self.smb_message.command, '<unknown>'), self.smb_message.command, self.smb_message.flags, self.smb_message.flags2, self.smb_message.tid, self.smb_message.uid)
                    pass

            if next_message_offset > 0:
                data = data[next_message_offset:]
            else:
                break

        return d

    def onNMBSessionMessageResponse(self, flags, data):
        if (not self.tree_connect_requests) and (not self.file_open_requests):
            # No pending requests means nothing to do
            return

        is_using_smb2 = False
        while True:
            try:
                i, smb_message = self.peekSMBMessageType(data)
            except SMB2ProtocolHeaderError:
                is_using_smb2 = True
                i, smb_message = self.peekSMB2MessageType(data)

            next_message_offset = 0
            if is_using_smb2:
                next_message_offset = smb_message.next_command_offset

            if i > 0:
                if is_using_smb2:
                    self._updateState_SMB2_Response(smb_message)
                else:
                    pass

            if next_message_offset > 0:
                data = data[next_message_offset:]
            else:
                break

    def peekSMBMessageType(self, buf):
        """Decode a SMB message.
        Returns (length_of_message_processed, smb_message)"""

        smb_message = SMBMessage()

        HEADER_STRUCT_FORMAT = "<4sBIBHHQxxHHHHB"
        HEADER_STRUCT_SIZE = struct.calcsize(HEADER_STRUCT_FORMAT)

        buf_len = len(buf)
        if buf_len < HEADER_STRUCT_SIZE:
            # We need at least 32 bytes (header) + 1 byte (parameter count)
            raise ProtocolError('Not enough data to decode SMB header', buf)

        protocol, smb_message.command, status, smb_message.flags, smb_message.flags2, pid_high, smb_message.security,\
            smb_message.tid, pid_low, smb_message.uid, smb_message.mid, params_count = \
            struct.unpack(HEADER_STRUCT_FORMAT, buf[:HEADER_STRUCT_SIZE])

        if protocol == '\xFESMB':
            raise SMB2ProtocolHeaderError()
        if protocol != '\xFFSMB':
            raise ProtocolError('Invalid 4-byte protocol field', buf)

        smb_message.pid = (pid_high << 16) | pid_low
        smb_message.status.internal_value = status
        smb_message.status.is_ntstatus = bool(smb_message.flags2 & SMB_FLAGS2_NT_STATUS)

        offset = HEADER_STRUCT_SIZE
        if buf_len < params_count * 2 + 2:
            # Not enough data in buf to decode up to body length
            raise ProtocolError('Not enough data. Parameters list decoding failed', buf)

        datalen_offset = offset + params_count * 2
        body_len = struct.unpack('<H', buf[datalen_offset:datalen_offset+2])[0]
        if body_len > 0 and buf_len < (datalen_offset + 2 + body_len):
            # Not enough data in buf to decode body
            raise ProtocolError('Not enough data. Body decoding failed', buf)

        smb_message.parameters_data = buf[offset:datalen_offset]

        if body_len > 0:
            smb_message.data = buf[datalen_offset+2:datalen_offset+2+body_len]

        smb_message.raw_data = buf

        return HEADER_STRUCT_SIZE + params_count * 2 + 2 + body_len, smb_message

    def peekSMB2MessageType(self, buf):
        """Decodes a SMB2 message.
        Returns (length_of_message_processed, smb_message)"""

        smb_message = SMB2Message()

        HEADER_STRUCT_FORMAT = '<4sHHIHHI'
        HEADER_STRUCT_SIZE = struct.calcsize(HEADER_STRUCT_FORMAT)
        HEADER_SIZE = 64
        ASYNC_HEADER_STRUCT_FORMAT = '<IQQQ16s'
        ASYNC_HEADER_STRUCT_SIZE = struct.calcsize(ASYNC_HEADER_STRUCT_FORMAT)
        SYNC_HEADER_STRUCT_FORMAT = '<IQIIQ16s'
        SYNC_HEADER_STRUCT_SIZE = struct.calcsize(SYNC_HEADER_STRUCT_FORMAT)

        buf_len = len(buf)
        if buf_len < 64:
            raise ProtocolError('Not enough data to decode SMB2 header', buf)

        protocol, struct_size, smb_message.credit_charge, smb_message.status, smb_message.command,\
            smb_message.credit_re, smb_message.flags = struct.unpack(HEADER_STRUCT_FORMAT, buf[:HEADER_STRUCT_SIZE])

        if protocol != '\xFESMB':
            raise ProtocolError('Invalid 4-byte SMB2 protocol field', buf)

        if struct_size != HEADER_SIZE:
            raise ProtocolError('Invalid SMB2 header structure size')

        isAsync = bool(smb_message.flags & SMB2_FLAGS_ASYNC_COMMAND)

        if isAsync:
            if buf_len < HEADER_STRUCT_SIZE+ASYNC_HEADER_STRUCT_SIZE:
                raise ProtocolError('Not enough data to decode SMB2 header', buf)

            smb_message.next_command_offset, smb_message.mid, smb_message.async_id, smb_message.session_id, \
                smb_message.signature = \
                struct.unpack(
                    ASYNC_HEADER_STRUCT_FORMAT,
                    buf[HEADER_STRUCT_SIZE:HEADER_STRUCT_SIZE+ASYNC_HEADER_STRUCT_SIZE]
                )
        else:
            if buf_len < HEADER_STRUCT_SIZE+SYNC_HEADER_STRUCT_SIZE:
                raise ProtocolError('Not enough data to decode SMB2 header', buf)
            smb_message.next_command_offset, smb_message.mid, smb_message.pid, smb_message.tid, smb_message.session_id,\
                smb_message.signature = \
                struct.unpack(
                    SYNC_HEADER_STRUCT_FORMAT,
                    buf[HEADER_STRUCT_SIZE:HEADER_STRUCT_SIZE+SYNC_HEADER_STRUCT_SIZE]
                )

        if smb_message.next_command_offset > 0:
            smb_message.raw_data = buf[:smb_message.next_command_offset]
            smb_message.data = buf[HEADER_SIZE:smb_message.next_command_offset]
        else:
            smb_message.raw_data = buf
            smb_message.data = buf[HEADER_SIZE:]

        return len(smb_message.raw_data), smb_message

    def _updateState_SMB2(self, message):
        """React to a SMB2 packet coming from the client.
        Returns either True, or a deferred that fires when the action has been processed."""
        if self.settings.LOG_SMB2_PACKETS and message.command != SMB2_COM_READ and message.command != SMB2_COM_WRITE:
            self.log.msg(
                'Processing SMB2 packet %d. Command: %s' % (message.mid, SMB2_COMMAND_NAMES[message.command]),
                level=logger.DEBUG
            )

        if message.command == SMB2_COM_CREATE:
            # print 'This is a create operation. Extracting filename...'

            request_share = self.check_message_tid(message)
            if request_share is not None:
                HEADER_SIZE = 64
                STRUCTURE_FORMAT = '<HBBIQQIIIIIHHII'
                STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

                structure_size, security_flag, oplock, impersonation, smb_create_flags, reserved, access_mask, \
                    file_attributes, share_access, create_disp, create_options, name_offset, \
                    name_length, create_context_offset, create_context_length \
                    = struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])

                # Whether there will be writes on this file.
                do_write = False

                # Whether the file will be deleted on close.
                do_delete = False

                if self.settings.LOG_SMB2_PACKETS:
                    self.log.msg("Requested access level is: %s" % repr(access_mask), level=logger.DEBUG)
                if self.settings.DEBUG_OUTPUT:
                    enabled_access_level = []
                    for opt in SMB_ACCESS_MASK_NAMES:
                        if access_mask & opt:
                            enabled_access_level.append(SMB_ACCESS_MASK_NAMES[opt])

                    self.log.msg("Requested access level: %s" % ("|".join(enabled_access_level)), level=logger.DEBUG)

                if access_mask & FILE_WRITE_DATA or \
                        access_mask & FILE_APPEND_DATA or \
                        access_mask & FILE_WRITE_ATTRIBUTES or \
                        access_mask & MAXIMUM_ALLOWED or \
                        access_mask & GENERIC_ALL or \
                        access_mask & GENERIC_WRITE:
                    do_write = True

                if self.settings.LOG_SMB2_PACKETS:
                    self.log.msg("Create options are: %s" % repr(create_options), level=logger.DEBUG)
                if self.settings.DEBUG_OUTPUT:
                    enabled_create_options = []
                    for opt in SMB_CREATE_OPTION_NAMES:
                        if create_options & opt:
                            enabled_create_options.append(SMB_CREATE_OPTION_NAMES[opt])

                    self.log.msg("Requested create_options: %s" % ("|".join(enabled_create_options)), level=logger.DEBUG)

                if create_options & FILE_DELETE_ON_CLOSE:
                    do_delete = True

                filename = message.raw_data[name_offset:name_offset+name_length].decode('UTF-16LE')
                # print 'Filename requested is "%s"' % (filename)
                # print 'mid=%d, tid=%d' % (message.mid, message.tid)

                # Parse the create context
                if self.settings.DEBUG_OUTPUT:
                    if create_context_offset != 0:
                        self.log.msg('Found CreateContext')
                        create_context_data = \
                            message.raw_data[create_context_offset:create_context_offset+create_context_length]

                        create_context_messages = []

                        STRUCTURE_FORMAT = '<IBBBBI'
                        STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

                        while True:
                            next, name_offset, name_length, reserved, data_offset, data_length = \
                                struct.unpack(STRUCTURE_FORMAT, create_context_data[:STRUCTURE_SIZE])

                            name = create_context_data[name_offset:name_offset+name_length]

                            if data_offset != 0 and data_length > 0:
                                data = create_context_data[data_offset:data_offset+data_length]
                            else:
                                data = None

                            create_context_messages.append((name, data))
                            if next == 0:
                                break
                            else:
                                create_context_data = create_context_data[next:]

                        self.log.msg(repr(create_context_messages))

                if request_share is not False:
                    d = self.syncFile(request_share, filename)

                    def log_error(error):
                        self.log.msg(error, level=logger.ERROR)
                    d.addErrback(log_error)

                    if do_write:
                        d.addCallback(lambda x: self.touch_file(request_share, filename))
                        d.addErrback(log_error)

                else:
                    self.log.msg(
                        'Error: Could not intercept request for file %s (message id: %s), request_share unknown.' % (
                            filename, repr(message.id)
                        ),
                        level=logger.ERROR
                    )
                    d = defer.succeed(None)

                def register_open_request(_):
                    if message.mid in self.file_open_requests:
                        self.log.msg(
                            'Message id reuse (%s) in file_open_requests. This should not be happening' % repr(message.id),
                            level=logger.ERROR
                        )

                    self.file_open_requests[message.mid] = {
                        'filename': filename,
                        'do_write': do_write,
                        'do_delete': do_delete,
                    }

                    self.session_latest_create_request_filename = filename

                d.addCallback(register_open_request)

                return d

        elif message.command == SMB2_COM_QUERY_DIRECTORY:
            request_share = self.check_message_tid(message)

            if request_share is not None:

                STRUCTURE_FORMAT = '<HBBI16sHHI'
                STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

                structure_size, file_information_class, flags, file_index, file_id, \
                    file_name_offset, file_name_length, output_buffer_length = \
                    struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])

                # self.log.msg(repr((
                #     structure_size, file_information_class, flags, file_index, file_id,
                #     file_name_offset, file_name_length, output_buffer_length
                # )))

                search_pattern = message.raw_data[file_name_offset:file_name_offset+file_name_length].decode('UTF-16LE')

                # Handle the file_information_class
                file_information_class_str = 'UNKNOWN'
                for v in SMB2_SMB2QueryDirectoryRequest_FileInformationClass_Values.keys():
                    if file_information_class == v:
                        file_information_class_str = SMB2_SMB2QueryDirectoryRequest_FileInformationClass_Values[v]
                        break

                flags_ary = [
                    SMB2_SMB2QueryDirectoryRequest_Flags_Values[v]
                    for v in SMB2_SMB2QueryDirectoryRequest_Flags_Values.keys()
                    if v & flags
                ]
                flags_str = '|'.join(flags_ary)

                if request_share is not False:
                    filename, _, _ = self.get_filename(file_id)
                    if filename is None:
                        self.log.msg(
                            'Error: could not find the filename associated to handle %s' % repr(file_id),
                            level=logger.ERROR
                        )
                    else:
                        if self.settings.DEBUG_OUTPUT:
                            self.log.msg(
                                'QueryDirectoryRequest: %s:%s %s %s "%s"' % (
                                    request_share,
                                    filename,
                                    file_information_class_str,
                                    flags_str,
                                    search_pattern,
                                ),
                                level=logger.INFO
                            )

                        d = self.listdir(request_share, filename)
                        d.addErrback(lambda x: self.log.msg(traceback.format_exc(x.value), level=logger.ERROR))
                        return d

        elif message.command == SMB2_COM_TREE_CONNECT:
            # print 'This is a tree_connect operation. Extracting requested host and share'
            # print 'mid=%d, tid=%d' % (message.mid, message.tid)

            HEADER_SIZE = 64
            STRUCTURE_FORMAT = '<HHHH'
            STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

            structure_size, reserved, path_offset, path_length = struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])

            path = message.raw_data[path_offset:path_offset+path_length].decode('UTF-16LE')
            # print 'Tree path requested is "%s"' % (path)
            self.tree_connect_requests[message.mid] = {'path': path}
            self.session_latest_tree_connect_path = path

        # elif message.command == SMB2_COM_WRITE:
        #     request_share = self.check_message_tid(message)
        #     if request_share is not None:
        #         STRUCTURE_FORMAT = "<HHIQ16sIIHHI"
        #         STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)
        #
        #         structure_size, data_offset, data_length, write_offset, file_id, channel, \
        #             remaining_bytes, write_channel_info_offset, write_channel_info_length, \
        #             flags = struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])
        #
        #         try:
        #             filename = self.open_files[file_id]['filename']
        #         except KeyError, e:
        #             self.log.msg(traceback.format_exc(), level=logger.ERROR)
        #             filename = "_"

        elif message.command == SMB2_COM_SET_INFO:
            request_share = self.check_message_tid(message)
            if request_share is not None:
                STRUCTURE_FORMAT = "<HBBIHHI16s"
                STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

                structure_size, info_type, file_info_class, buffer_length, buffer_offset, \
                    _, additional_info, file_id = struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])

                try:
                    filename = self.open_files[file_id]['filename']
                except KeyError, e:
                    self.log.msg(traceback.format_exc(), level=logger.ERROR)
                    filename = "_"
                else:
                    # self.log.msg(filename, level=logger.DEBUG)
                    # self.log.msg("Info_type: %d" % info_type, level=logger.DEBUG)
                    # self.log.msg("File_info_class: %d" % file_info_class, level=logger.DEBUG)

                    data = message.data[STRUCTURE_SIZE:STRUCTURE_SIZE + buffer_length]
                    # self.log.msg("Data length: %d" % len(data), level=logger.DEBUG)

                    if info_type == 1:
                        if file_info_class == 4:
                            pass
                            # self.log.msg("FileBasicInformation -> Not supported", level=logger.DEBUG)
                        elif file_info_class == 13:
                            # self.log.msg("FileDispositionInformation", level=logger.DEBUG)
                            buffer_format = "<B"
                            (do_delete,) = struct.unpack(buffer_format, data)
                            if do_delete == 1:
                                # self.log.msg("Setting do_delete to 1 as requested by SET_INFO", level=logger.INFO)
                                self.open_files[file_id]['do_delete'] = True
                            elif do_delete == 0:
                                # self.log.msg("Setting do_delete to 0 as requested by SET_INFO", level=logger.INFO)
                                self.open_files[file_id]['do_delete'] = False
                            else:
                                self.log.msg("Unrecognized value for do_delete: %d" % do_delete, level=logger.INFO)
                        elif file_info_class == 20:
                            pass
                            # self.log.msg("FileEndOfFileInformation -> Uninteresting", level=logger.DEBUG)
                        else:
                            pass
                            # self.log.msg("Unsupported FileInfoClass", level=logger.DEBUG)

        elif message.command == SMB2_COM_CLOSE:
            request_share = self.check_message_tid(message)
            if request_share is not None:
                STRUCTURE_FORMAT = "<HHI16s"
                STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

                structure_size, flags, _, file_id = struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])

                try:
                    filename = self.open_files[file_id]['filename']
                    do_write = self.open_files[file_id]['do_write']
                    do_delete = self.open_files[file_id]['do_delete']
                except KeyError, e:
                    self.log.msg(traceback.format_exc(), level=logger.ERROR)
                    filename = "_"
                    do_write = False
                    do_delete = False

                # self.log.msg("Closing file %s" % (filename,), level=logger.DEBUG)

                d = defer.succeed(None)

                if do_write:
                    # self.log.msg("Got do_write", level=logger.DEBUG)
                    # Sync back the file
                    d.addCallback(lambda x: self.sync_back_file(request_share, filename))
                    d.addErrback(lambda x: self.log.msg(traceback.format_exc(x.value), level=logger.INFO))

                if do_delete:
                    # self.log.msg("Got do_delete", level=logger.DEBUG)
                    # Delete the file
                    d.addCallback(lambda x: self.delete_file(request_share, filename))
                    d.addErrback(lambda x: self.log.msg(traceback.format_exc(x.value), level=logger.INFO))

                def remove_handle_from_open_files(_):
                    # Remove the handle from the list of open files.
                    try:
                        del self.open_files[file_id]
                    except KeyError:
                        pass

                d.addCallback(remove_handle_from_open_files)
                d.addBoth(self.tryShutdown)

                return d

        return True

    def _updateState_SMB2_Response(self, message):
        """React to a SMB2 message coming from the server"""
        if self.settings.LOG_SMB2_PACKETS and message.command != SMB2_COM_READ and message.command != SMB2_COM_WRITE:
            self.log.msg(
                'Got response SMB2 packet. Command: %s' % SMB2_COMMAND_NAMES[message.command],
                level=logger.DEBUG
            )

        if message.command == SMB2_COM_TREE_CONNECT:
            # print 'This is the response to a tree_connect attempt'
            # print 'mid=%d, tid=%d' % (message.mid, message.tid)
            try:
                self.connected_trees[message.tid] = self.tree_connect_requests[message.mid]
                del self.tree_connect_requests[message.mid]
            except Exception:
                pass

        elif message.command == SMB2_COM_CREATE:
            request_share = self.check_message_tid(message)
            if request_share is not None:
                STRUCTURE_FORMAT = "<HBBIQQQQQQII16sII"
                STRUCTURE_SIZE = struct.calcsize(STRUCTURE_FORMAT)

                if message.status == 0:
                    struct_size, oplock, _, create_action, \
                    create_time, lastaccess_time, lastwrite_time, change_time, \
                    allocation_size, file_size, file_attributes, \
                    _, fid, _, _ = struct.unpack(STRUCTURE_FORMAT, message.data[:STRUCTURE_SIZE])

                    try:
                        self.open_files[fid] = self.file_open_requests[message.mid]
                        self.open_files[fid]['open_datetime'] = datetime.utcnow().isoformat()
                        # self.log.msg("Associating %s to file %s" %
                        #             (repr(fid), self.open_files[fid]['filename'].encode('UTF-8')), level=logger.DEBUG)
                        del self.file_open_requests[message.mid]
                    except Exception, e:
                        self.log.msg(traceback.format_exc(), level=logger.INFO)
                        self.log.msg(
                            "Warning: got SMB2_COM_CREATE response but could not map back to a requested filename.",
                            level=logger.WARN)

                else:
                    if message.status in SMB2_NTSTATUS_ERRORS.keys():
                        error_message = SMB2_NTSTATUS_ERRORS[message.status]
                    else:
                        error_message = "0x%08x" % message.status

                    # If the request failed, we still need to remove it from the list of pending requests
                    if message.mid in self.file_open_requests:
                        requested_filename = self.file_open_requests[message.mid]['filename']
                        del self.file_open_requests[message.mid]
                    else:
                        requested_filename = None
                    self.log.msg(
                        "SMB2_COM_CREATE response with status %s. Ignoring." % error_message,
                        requested_filename=requested_filename,
                        level=logger.INFO)

        return True

    def get_filename(self, file_id):
        """
        :param file_id: the requested file_id
        :return: (filename or None, do_write, do_delete)
        """
        if file_id == "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff":
            if self.session_latest_create_request_filename is not None:
                return self.session_latest_create_request_filename, False, False
            else:
                self.log.msg(
                    'Error: File_id indicates a compound message, but could not map back to a filename.',
                    level=logger.ERROR
                )
                return None, False, False

        try:
            filename = self.open_files[file_id]['filename']
            do_write = self.open_files[file_id]['do_write']
            do_delete = self.open_files[file_id]['do_delete']
            return filename, do_write, do_delete
        except KeyError:
            self.log.msg(traceback.format_exc(), level=logger.ERROR)
            return None, False, False

    def tryShutdown(self, v=None):
        do_shutdown = True
        for f in self.open_files:
            if self.open_files[f]['filename'] != '':
                do_shutdown = False
                break

        if do_shutdown:
            if self.shutdown_deferred is not None:
                self.shutdown_deferred.callback(None)
                self.shutdown_deferred = None

        return v

    def syncFile(self, full_share, path):
        """Tells the backend to sync a file. Returns a deferred that fires when the action is done."""
        d = defer.succeed((full_share, path, self.log))
        if path == 'srvsvc':
            # Special case, we don't touch that
            return d

        def process(args):
            return self.fscacheclient.sync(*args)

        d.addCallback(process)

        return d

    def listdir(self, full_share, path):
        """Tells the backend to sync the contents of a directory.
        Returns a deferred that fires when the action is done."""
        d = defer.succeed((full_share, path, self.log))
        if path == 'srvsvc':
            return d

        def process(args):
            return self.fscacheclient.listdir(*args)

        d.addCallback(process)

        return d

    def sync_back_file(self, full_share, path):
        """Tells the backend to write back a file. Returns a deferred that fires when the action is done."""
        d = defer.succeed((full_share, path, self.log))
        if not self.settings.ENABLE_WRITE_THROUGH:
            return d

        if path == 'srvsvc':
            return d

        def process(args):
            return self.fscacheclient.sync_back(*args)
        d.addCallback(process)

        return d

    def delete_file(self, full_share, path):
        """Tells the backend to delete a file. Returns a deferred that fires when the action is done."""
        d = defer.succeed((full_share, path, self.log))
        if not self.settings.ENABLE_WRITE_THROUGH:
            return d

        if path == 'srvsvc':
            return d

        def process(args):
            return self.fscacheclient.delete(*args)
        d.addCallback(process)

        return d

    def touch_file(self, full_share, path):
        """Tells the backend to touch a file. Returns a deferred that fires when the action is done."""
        d = defer.succeed((full_share, path, self.log))
        if not self.settings.ENABLE_WRITE_THROUGH:
            return d

        if not self.settings.ENABLE_TOUCH_FILES:
            return d

        if path == 'srvsvc':
            return d

        def process(args):
            return self.fscacheclient.touch(*args)
        d.addCallback(process)

        return d

    def check_message_tid(self, message):
        """Returns the share that this message corresponds to.
        Returns None if the share is unknown or if it corresponds to a passthrough share"""
        try:
            if message.tid == 0xFFFFFFFF:
                if self.session_latest_tree_connect_path is not None:
                    request_share = self.session_latest_tree_connect_path
                else:
                    self.log.msg(
                        'Error: Tree_id indicates a compound message, but could not map back to a share',
                        level=logger.ERROR,
                    )
                    request_share = False
            else:
                request_share = self.connected_trees[message.tid]['path']
        except Exception:
            error_message = 'Request share (requested: %s) unknown. This is weird. Known shares: %s.' % (
                repr(message.tid),
                ','.join([repr(v) for v in self.connected_trees.keys()])
            )
            self.log.msg(error_message, level=logger.INFO)
            return False
        else:
            if not self.share_is_intercepted(request_share):
                return None
            else:
                # HOTFIX
                split_request_share = request_share.split('\\')
                split_request_share[2] = "WIN-9LHJ7FU43T7"
                request_share = "\\".join(split_request_share)

                return request_share


class ProxyServerFactory(protocol.Factory):
    protocol = ProxyServerProtocol

    def __init__(self, fscache, fscacheclient, settings):
        # self.fscache = FSCache()
        # self.fscacheclient = FSLocalCacheClient(self.fscache)
        self.fscache = fscache
        self.fscacheclient = fscacheclient
        self.settings = settings

        self.clients = []

        self.shutdown_requested = False

    @defer.inlineCallbacks
    def shutdown(self):
        self.shutdown_requested = True
        # Mark all clients as needing to shutdown
        dl = defer.DeferredList([
            client.shutdown()
            for client in self.clients
        ])

        # Wait until we have no clients left trying to connect
        yield dl


class ManagementInterfaceProtocol(LineReceiver):
    """This defines a very basic management API over raw TCP messages.

    Supported messages:
    * RESETL3: resets the filesystem shared by samba
    * RESETL2: resets the metadata cache kept in memory
    """

    delimiter = '\n'

    def lineReceived(self, line):
        if line == 'STATS':
            d = get_debug_stats_struct(
                self.factory.proxy_factory,
                self.factory.proxy_listen_address,
                self.factory.proxy_listen_port
            )
            ret = json.dumps(d, indent=4)
            self.transport.write(ret)
            self.transport.loseConnection()
        elif line == 'SHUTDOWN':
            initiateShutdown(self.factory.proxy_port, self.factory.proxy_factory)
            self.transport.write('OK')
            self.transport.loseConnection()


class ManagementInterfaceFactory(protocol.Factory):
    protocol = ManagementInterfaceProtocol

    def __init__(self, port, factory, listen_address, listen_port):
        self.fscacheclient = factory.fscacheclient
        self.proxy_port = port
        self.proxy_factory = factory
        self.proxy_listen_address = listen_address
        self.proxy_listen_port = listen_port


def init(
        listen_address,
        listen_port,
        fileserver_address,
        fileserver_port,
        metadata_proxy_address,
        metadata_proxy_port,
        settings):
    # Initialize the caches
    fscache = FSCache(
        settings,
        host=fileserver_address,
        port=fileserver_port,
        metadata_proxy_host=metadata_proxy_address,
        metadata_proxy_port=metadata_proxy_port,
        redis_host=settings.REDIS_FILE_TRANSFERBACK_HOST
    )
    fscacheclient = FSLocalCacheClient(fscache, settings)

    # Initialize the proxy
    factory = ProxyServerFactory(fscache, fscacheclient, settings)
    port = reactor.listenTCP(listen_port, factory, interface=listen_address)

    # management_factory = ManagementInterfaceFactory(fscache, fscacheclient)
    # reactor.listenTCP(40445, management_factory, interface='0.0.0.0')

    management_factory = ManagementInterfaceFactory(port, factory, listen_address, listen_port)
    reactor.listenUNIX('/tmp/smbproxy-%d.sock' % os.getpid(), management_factory)

    if settings.ENABLE_CENTRAL_STATS_FORWARD:
        stream_stats_client = StreamStatsClient(settings.CENTRAL_STATS_SERVER_HOST)
    else:
        stream_stats_client = None

    # Setup periodic dump
    periodic_stats_dump = LoopingCall(
        dump_debug_stats,
        factory,
        listen_address,
        listen_port,
        stream_stats_client,
    )
    periodic_stats_dump.start(1)

    reactor.run()


@defer.inlineCallbacks
def initiateShutdown(port, factory):
    yield defer.maybeDeferred(port.stopListening)

    yield factory.shutdown()

    reactor.stop()
