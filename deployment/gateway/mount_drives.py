# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import luna_commons
import logging
import posixpath
import ntpath

#from api_client import GatewayAPIClient
import settings

SEEKSCALE_MOUNTPOINTS_ROOT = settings.SEEKSCALE_MOUNTPOINTS_ROOT


class MountPoint(object):
    normalized_unc_path = None

    def __init__(self, unc):
        self.normalized_unc_path = self.normalize_unc_path(unc)

    @classmethod
    def normalize_unc_path(cls, unc_path):
        # Remove any remaining path part
        unc, path = ntpath.splitunc(unc_path)
        if unc != unc_path:
            logging.warning('Got unc_path %s which is non-canonical. Using %s instead' % (
                unc_path,
                unc
            ))

        # Lowercase the whole host/service
        unc = unc.lower()

        return unc

    @property
    def linux_formatted_unc(self):
        """Returns the UNC path undef the //host/share format"""
        linux_formatted_unc = self.normalized_unc_path.replace('\\', '/')
        return linux_formatted_unc

    @classmethod
    def get_mounts(cls):
        with open('/proc/mounts', 'r') as fh:
            procmounts = fh.read()

        mounts = []

        for mount_line in procmounts.splitlines():
            (dev, mountpoint, t, options, _, _) = mount_line.split()

            mounts.append((dev, mountpoint, t, options))

        return mounts

    def is_mounted(self, mount_data=None):
        if mount_data is None:
            mount_data = self.get_mounts()

        for (dev, mountpoint, t, options) in mount_data:
            if dev == self.linux_formatted_unc:
                return True

        return False

    @property
    def mountpoint(self):
        unc_path = self.normalized_unc_path

        # First, try to see if this unc path is statically mounted
        mountpoint = settings.unc_static_mappings.get(unc_path, None)

        # Otherwise, auto-generate a local path.
        if mountpoint is None:
            (_, _, host, service) = unc_path.lower().split('\\')
            mountpoint = posixpath.join(SEEKSCALE_MOUNTPOINTS_ROOT, host, service)
        return mountpoint

    def mount(self):
        if self.is_mounted():
            logging.info('Drive %s already mounted. Nothing to do.' % self.normalized_unc_path)
            return

        # Create the mountpoint
        luna_commons.create_dir(self.mountpoint)

        # Do the mount
        cmd = [
            'mount.cifs',
            self.linux_formatted_unc,
            self.mountpoint,
            '-o',
            'credentials=%s' % (settings.SMB_CREDENTIALS_FILE,)
        ]

        retcode, out, err = luna_commons.exec_command(cmd)

        if retcode == 0:
            logging.info('Successfully mounted %s as %s' % (
                self.normalized_unc_path,
                self.mountpoint
            ))
            return True
        else:
            logging.error('Could not mount %s: %d [%s][%s]' % (
                self.normalized_unc_path,
                retcode,
                out,
                err
            ))
            return False

    def umount(self):
        if not self.is_mounted():
            logging.info('Drive %s not mounted. Nothing to unmount.' % self.normalized_unc_path)
            return

        # Format the UNC path for mount
        cmd = [
            'umount',
            self.mountpoint
        ]

        retcode, out, err = luna_commons.exec_command(cmd)

        if retcode == 0:
            logging.info('Successfully unmounted %s from %s' % (
                self.normalized_unc_path,
                self.mountpoint
            ))
            return True
        else:
            logging.error('Could not unmount %s: %d [%s][%s]' % (
                self.normalized_unc_path,
                retcode,
                out,
                err
            ))
            return False

# def mountpoint_from_unc(unc_path):
#     # First, try to see if this unc path is statically mounted
#     mountpoint = settings.unc_static_mappings.get(unc_path, None)
#
#     # Otherwise, auto-generate a local path.
#     if mountpoint is None:
#         (_, _, host, service) = unc_path.lower().split('\\')
#         mountpoint = posixpath.join(SEEKSCALE_MOUNTPOINTS_ROOT, host, service)
#     return mountpoint


# def normalize_unc_path(unc_path):
#     # Get the canonical UNC path
#     unc, path = ntpath.splitunc(unc_path)
#     if unc != unc_path:
#         logging.warning('Got unc_path %s which is non-canonical. Using %s instead' % (
#             unc_path,
#             unc
#         ))
#     return unc


# def linux_formatted_unc_path(unc_path):
#     """Transforms an unc path into the //host/share format"""
#     linux_formatted_unc = unc_path.replace('\\', '/')
#     return linux_formatted_unc


# def mount_win_drive(unc_path):
#     if is_mounted_win_drive(unc_path):
#         logging.info('Drive %s already mounted. Nothing to do.' % unc_path)
#         return
#
#     # Format the UNC path for mount
#     unc = normalize_unc_path(unc_path)
#     linux_formatted_unc = linux_formatted_unc_path(unc)
#
#     mountpoint = mountpoint_from_unc(unc)
#     luna_commons.create_dir(mountpoint)
#
#     # Do the mount
#     cmd = [
#         'mount.cifs',
#         linux_formatted_unc,
#         mountpoint,
#         '-o',
#         'credentials=%s' % (settings.SMB_CREDENTIALS_FILE,)
#     ]
#
#     retcode, out, err = luna_commons.exec_command(cmd)
#
#     if retcode == 0:
#         logging.info('Successfully mounted %s as %s' % (
#             unc,
#             mountpoint
#         ))
#         return True
#     else:
#         logging.error('Could not mount %s: %d [%s][%s]' % (
#             unc,
#             retcode,
#             out,
#             err
#         ))
#         return False


# def unmount_win_drive(unc_path):
#     if not is_mounted_win_drive(unc_path):
#         logging.info('Drive %s not mounted. Nothing to unmount.' % unc_path)
#         return
#
#     # Format the UNC path for mount
#     unc = normalize_unc_path(unc_path)
#
#     mountpoint = mountpoint_from_unc(unc)
#
#     cmd = [
#         'umount',
#         mountpoint
#     ]
#
#     retcode, out, err = luna_commons.exec_command(cmd)
#
#     if retcode == 0:
#         logging.info('Successfully unmounted %s from %s' % (
#             unc,
#             mountpoint
#         ))
#         return True
#     else:
#         logging.error('Could not unmount %s: %d [%s][%s]' % (
#             unc,
#             retcode,
#             out,
#             err
#         ))
#         return False


# def get_mounts():
#     with open('/proc/mounts', 'r') as fh:
#         procmounts = fh.read()
#
#     mounts = []
#
#     for mount_line in procmounts.splitlines():
#         (dev, mountpoint, type, options, _, _) = mount_line.split()
#
#         mounts.append((dev, mountpoint, type, options))
#
#     return mounts


# def is_mounted_win_drive(unc_path):
#     unc = normalize_unc_path(unc_path)
#     linux_formatted_unc = linux_formatted_unc_path(unc)
#
#     mounts = get_mounts()
#
#     for (dev, mountpoint, type, options) in mounts:
#         if dev == linux_formatted_unc:
#             return True
#
#     return False


def test():
    luna_commons.setup_logging(level=logging.INFO)
    p = '\\\\seekscale.local\\Q'

    mount = MountPoint(p)
    mount.mount()
    mount.umount()


def mount_all_drives_api():
    luna_commons.setup_logging(level=logging.INFO)

    api_client = GatewayAPIClient()

    for drive in api_client.get_drives():
        unc = '\\\\%s\\%s' % (drive['host'], drive['service_name'],)
        mount = MountPoint(unc)
        mount.mount()


def register_credentials():
    print("This will register the credentials used to connect to SMB shared drives")
    print("These credentials will be stored on this machine. They will never be transmitted anywhere else.")
    username = raw_input("SMB Username: ")
    password = raw_input("SMB Password: ")
    domain = raw_input("SMB Domain (leave blank if no domain): ")

    if len(username) == 0:
        username = None
    if len(password) == 0:
        password = None
    if len(domain) == 0:
        domain = None

    with open(settings.SMB_CREDENTIALS_FILE, 'w') as fh:
        fh.write("username=%s\n" % username)
        fh.write("password=%s\n" % password)
        if domain is not None:
            fh.write("domain=%s\n" % domain)

    print("Credentials successfully registered!")
