# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""Evented uploader for the raw-nginx based cache server"""


from cStringIO import StringIO
import hashlib
import json
import logging
import math
import os
import shutil
import sys
import tempfile

from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks, returnValue
from twisted.internet.protocol import Protocol
from twisted.internet.ssl import Certificate, PrivateCertificate, optionsForClientTLS
from twisted.python.filepath import FilePath
from twisted.web.client import Agent
from twisted.web.client import BrowserLikePolicyForHTTPS, _requireSSL
from twisted.web.client import readBody
from twisted.web.client import FileBodyProducer
from twisted.web.client import HTTPConnectionPool
from twisted.web.client import ResponseDone
from twisted.web.http_headers import Headers

# Make some imports explicit, to help pyinstaller
from OpenSSL import crypto


logger = logging.getLogger(__name__)

CHUNK_SIZE_IN_MB = 5
CONNECTION_COUNT = 50


def sha256sum_str(data):
    """Returns that SHA256 checksum of a binary string"""
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()


class BrowserLikePolicyForHTTPSWithClientCertificate(BrowserLikePolicyForHTTPS):
    """Extend the default HTTPS certificate policy to send a given SSL certificate"""
    def __init__(self, trustRoot=None, clientCertificate=None):
        self._trustRoot = trustRoot
        self._clientCertificate = clientCertificate

    @_requireSSL
    def creatorForNetloc(self, hostname, port):
        return optionsForClientTLS(
            hostname.decode("ascii"),
            trustRoot=self._trustRoot,
            clientCertificate=self._clientCertificate)


class _ReadBodyToTempFileProtocol(Protocol):
    def __init__(self, status, message, deferred):
        self.deferred = deferred
        self.status = status
        self.message = message

        fd, self.tempfile = tempfile.mkstemp()
        self.fh = os.fdopen(fd, 'wb')

    def dataReceived(self, data):
        self.fh.write(data)

    def connectionLost(self, reason):
        self.fh.close()
        if reason.check(ResponseDone):
            self.deferred.callback(self.tempfile)
        else:
            try:
                os.remove(self.tempfile)
            except Exception:
                logger.warn('Could not cleanup temporary file %s')
            self.deferred.errback(reason)


def readBodyToTempFile(response):
    """
    Same as the standard twisted.web.client.readBody, except it yields the path to a
    temporary file where the response has been written.
    :param response:
    :return:
    """
    def cancel(deferred):
        getattr(protocol.transport, 'abortConnection', lambda: None)()

    d = Deferred(cancel)
    protocol = _ReadBodyToTempFileProtocol(response.code, response.phrase, d)
    response.deliverBody(protocol)
    return d


def create_agent(ca_cert, client_cert, client_key):
    ca_certificate = Certificate.loadPEM(FilePath(ca_cert).getContent())
    client_certificate = PrivateCertificate.loadPEM(
        FilePath(client_cert).getContent() + b"\n" +
        FilePath(client_key).getContent())

    customPolicy = BrowserLikePolicyForHTTPSWithClientCertificate(
        trustRoot=ca_certificate,
        clientCertificate=client_certificate)

    pool = HTTPConnectionPool(reactor, persistent=True)
    pool.maxPersistentPerHost = CONNECTION_COUNT
    agent = Agent(reactor, customPolicy, pool=pool)

    return agent


def upload_part(data, shasum, uid, offset, length, agent=None):
    file_size = length
    file_shasum = shasum

    # Check if the chunk already exists
    d0 = check_part(file_shasum, agent=agent)

    def cbCheckPart(has_file):
        if has_file:
            return None
        else:

            def run_upload_part():
                f = StringIO(data)

                body = FileBodyProducer(f)
                d = agent.request(
                    'POST',
                    'https://entrypoint.seekscale.com:34968/upload',
                    Headers({
                        'X-Seekscale-Payload-Length': [str(file_size)],
                        'X-Seekscale-Payload-Shasum': [file_shasum],
                    }),
                    body)

                def cbResponse(response):
                    if response.code != 200:
                        # Always read the body
                        body_d = readBody(response)

                        def raiseError(_):
                            raise RuntimeError('Bad status code (%d) while upload file part %d' % (response.code, uid))
                        body_d.addBoth(raiseError)
                        return body_d
                    else:
                        d = readBody(response)
                        return d

                def cbPrintJsonBody(r):
                    logger.info(r)
                    return None
                d.addCallback(cbResponse)
                d.addCallback(cbPrintJsonBody)

                return d

            if agent.deferred_semaphore is not None:
                d2 = agent.deferred_semaphore.run(run_upload_part)
            else:
                d2 = run_upload_part()

            return d2

    def cbBuildReturnValue(_):
        return {
           'uid': uid,
           'offset': offset,
           'length': length,
           'shasum': file_shasum,
        }

    d0.addCallback(cbCheckPart)
    d0.addCallback(cbBuildReturnValue)

    return d0


def download_part(shasum, agent=None):
    def run_download_part():
        d = agent.request(
            'GET',
            'https://entrypoint.seekscale.com:34968/get/%s' % str(shasum))

        def cbResponse(response):
            if response.code != 200:
                # Always read the body
                body_d = readBody(response)

                def raiseError(_):
                    raise RuntimeError('Bad status code (%d) while downloading file' % response.code)

                body_d.addBoth(raiseError)
                return body_d
            else:
                d = readBody(response)
                return d

        d.addCallback(cbResponse)

        return d

    if agent.deferred_semaphore is not None:
        d2 = agent.deferred_semaphore.run(run_download_part)
    else:
        d2 = run_download_part()

    return d2


def download_part_to_disk(shasum, agent=None):
    def run_download_part_to_disk():
        d = agent.request(
            'GET',
            'https://entrypoint.seekscale.com:34968/get/%s' % str(shasum)
        )

        def cbResponse(response):
            if response.code != 200:
                # Always ready the body
                body_d = readBody(response)

                def raiseError(_):
                    raise RuntimeError('Bad status code (%d) while downloading file' % response.code)

                body_d.addBoth(raiseError)
                return body_d
            else:
                tmp_file = readBodyToTempFile(response)
                return tmp_file

        d.addCallback(cbResponse)

        return d

    if agent.deferred_semaphore is not None:
        d2 = agent.deferred_semaphore.run(run_download_part_to_disk)
    else:
        d2 = run_download_part_to_disk()

    return d2


def check_part(shasum, agent=None):
    def run_check_part():
        d = agent.request('HEAD', 'https://entrypoint.seekscale.com:34968/get/%s' % str(shasum))

        def cbResponse(response):
            if response.code == 200:
                return True
            else:
                return False
        d.addCallback(cbResponse)

        return d

    if agent.deferred_semaphore is not None:
        d2 = agent.deferred_semaphore.run(run_check_part)
    else:
        d2 = run_check_part()

    return d2


def read_chunk(fh, length):
    d = threads.deferToThread(fh.read, length)

    def cb(chunk):
        if chunk == '':
            return None
        else:
            return chunk, sha256sum_str(chunk)
    d.addCallback(cb)

    return d


@inlineCallbacks
def upload(path, agent=None):
    """
    Splits and upload the file given by path
    Returns a Deferred that fires the manifest object.
    """
    total_size = os.path.getsize(path)
    chunk_size = CHUNK_SIZE_IN_MB*1024*1024

    parts = int(math.ceil(float(total_size)/float(chunk_size)))
    logger.info("%d parts" % parts)

    uid = 0
    offset = 0
    final_data = []

    with open(path, 'rb') as f:

        while offset < total_size:
            tasks = []
            queued_tasks = 0

            while queued_tasks < CONNECTION_COUNT and offset < total_size:

                if offset + chunk_size <= total_size:
                    length = chunk_size
                else:
                    length = total_size - offset

                deferred_data = read_chunk(f, length)
                data, shasum = yield deferred_data
                d = upload_part(data, shasum, uid, offset, length, agent=agent)
                queued_tasks += 1
                tasks.append(d)

                offset += length
                uid += 1

            final_data += yield DeferredList(tasks)

    for (r, res) in final_data:
        if r is not True:
            returnValue(res)

        returnValue([b for (a, b) in final_data])


@inlineCallbacks
def download_with_tmp_files(manifest, output_path, agent=None):
    """
    Downloads the file described by the manifest object to path output_path
    Returns a Deferred that fires when the download has completed. (it fires nothing)
    :param manifest:
    :param output_path:
    :param agent:
    :return:
    """
    sorted_manifest = sorted(manifest, key=lambda k: k['uid'])
    final_data = []

    tasks = []
    queued_tasks = 0
    for part in sorted_manifest:
        d = download_part_to_disk(part['shasum'], agent=agent)
        tasks.append(d)
        queued_tasks += 1

        if queued_tasks >= CONNECTION_COUNT:
            final_data += yield DeferredList(tasks, consumeErrors=True)

            tasks = []
            queued_tasks = 0

    # Process the remaining tasks
    if queued_tasks > 0:
        final_data += yield DeferredList(tasks, consumeErrors=True)

    def build_output_file(data):
        success = True
        result = None

        # Check if all the parts were successful
        for (r, res) in data:
            if r is not True:
                result = res
                success = False
                break

        # If we have all the parts, combine them into the output file
        if success:
            with open(output_path, 'wb') as f:
                for (r, res) in data:
                    with open(res, 'rb') as f_source:
                        shutil.copyfileobj(f_source, f, 10240)
            logger.info("Downloaded file size: %d" % os.path.getsize(output_path))

        # Whatever the result, we clean up all the successful temporary files
        for (r, res) in data:
            if r is True:
                try:
                    os.remove(res)
                except Exception:
                    logger.warn('Could not cleanup temporary file %s' % res)

        returnValue(result)

    build_output_file(final_data)


#
# From here on, examples of usage of the API
#

def main_upload(path):
    server_cert = '/tmp/seekscale_swift_certs/ca.crt'
    client_cert = '/tmp/seekscale_swift_certs/client.crt'
    client_key = '/tmp/seekscale_swift_certs/client.key'
    agent = create_agent(server_cert, client_cert, client_key)

    d = upload(path, agent=agent)

    def dump_result(res):
        with open('result.json', 'wb') as f:
            f.write(json.dumps(res))
    d.addCallback(dump_result)

    def handleError(error):
        print "An error occured while uploading the file:", error
    d.addErrback(handleError)

    def cbShutdown(_):
        reactor.stop()
    d.addBoth(cbShutdown)

    reactor.run()


def main_download(manifest_file, output_path):
    server_cert = '/tmp/seekscale_swift_certs/ca.crt'
    client_cert = '/tmp/seekscale_swift_certs/client.crt'
    client_key = '/tmp/seekscale_swift_certs/client.key'
    agent = create_agent(server_cert, client_cert, client_key)

    with open(manifest_file, 'rb') as f:
        manifest = json.loads(f.read())
    d = download_with_tmp_files(manifest, output_path, agent=agent)

    def handleError(error):
        print "An error occured while downloading the file:", error
    d.addErrback(handleError)

    def cbShutdown(_):
        reactor.stop()
    d.addBoth(cbShutdown)

    reactor.run()


if __name__ == '__main__':
    if sys.argv[1] == 'upload':
        main_upload(sys.argv[2])
    elif sys.argv[1] == 'download':
        main_download(sys.argv[2], sys.argv[3])
