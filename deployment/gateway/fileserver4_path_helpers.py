# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import logging
import ntpath
import os
import platform
import posixpath

import mount_drives
import settings


def translate_path_linux(p):
    def linux_path_from_unc_path(unc_share, sub_path):
        mount = mount_drives.MountPoint(unc_share)
        unix_mountpoint = unicode(mount.mountpoint)

        if len(sub_path) == 0:
            ret = unix_mountpoint
        else:
            # Remove leading \ in the path part
            while len(sub_path) > 0 and sub_path[0] == u'\\':
                sub_path = sub_path[1:]

            ret = posixpath.join(unix_mountpoint, sub_path.replace(u'\\', u'/'))

        return ret

    # UNC path ?
    if p.startswith(u'\\\\'):
        unc, path = ntpath.splitunc(p)

        return linux_path_from_unc_path(unc, path)

    # Windows path with drive ?
    drive, path = ntpath.splitdrive(p)
    if drive != '':
        try:
            unc = unicode(settings.drives_mapping[drive.upper()])
        except KeyError:
            pass
        else:
            return linux_path_from_unc_path(unc, path)

    return p


def translate_path(p):
    """Handles path translation:
    This only does significant works when the gateway is running linux and the client asks for a Windows path
    In this case, it maps to the associated mounted drive.

    In any other case, it passes the path unmodified.

    Path is assumed to be unicode, and this function returns unicode."""
    if platform.system() == 'Linux':
        return translate_path_linux(p)

    return p


def cached_listdir(dirpath, cache=None):
    """A version of listdir that supports a local cache.
    Used for when we have to do listdir() a lot of times, with mostly the same arguments.
    It returns a map (dir.lower() -> dir) because, when done at a high rate, dir.lower() can become a bottleneck"""
    if cache is None:
        cache = {}

    if dirpath not in cache:
        files_list = listdir(dirpath)

        cache[dirpath] = {}
        for f in files_list:
            cache[dirpath][f.lower()] = f

    return cache[dirpath]


def normalize_case_linux(name, listdir_cache):
    """Normalizes the case of a path.

    On a case-insensitive filesystem, this returns the case that exists on the filesystem for a given path.
    Note: This is linux-specific.

    Works only for existing files.

    Name should be a UNC path, in unicode.
    """

    try:
        if name.startswith(u'\\\\'):
            unc, path = ntpath.splitunc(name)

    #        return linux_path_from_unc_path(unc, path).encode('UTF-8')

            mount = mount_drives.MountPoint(unc)
            base_mount = unicode(mount.mountpoint)

            path = path.replace(u'/', u'\\')
            dirs = path.split(u'\\')

            curpath = u''
            for d in dirs[1:]:
                curpath += u'\\'
                if d != u'':
                    unix_dir = os.path.join(base_mount, curpath[1:].replace(u'\\', u'/'))
                    potential_children = cached_listdir(unix_dir, cache=listdir_cache)
                    curpath += potential_children[d.lower()]

            return curpath

        else:
            return None

    except Exception:
        logging.exception('Could not normalize case:')
        return None


def normalize_case_windows(name, listdir_cache):
    try:
        if name.startswith(u'\\\\'):
            unc, path = ntpath.splitunc(name)

            dirs = path.split(u'\\')

            curpath = u''
            for d in dirs[1:]:
                curpath += u'\\'
                if d != u'':
                    windows_dir = os.path.join(unc, curpath[1:])
                    potential_children = cached_listdir(windows_dir, cache=listdir_cache)
                    curpath += potential_children[d.lower()]

            return curpath

        else:
            return None

    except Exception:
        logging.exception('Could not normalize case:')
        return None


if platform.system() == 'Linux':
    normalize_case = normalize_case_linux
elif platform.system() == 'Windows':
    normalize_case = normalize_case_windows
else:
    raise RuntimeError('Unsupported platform')


# This is a workaround because pyinstaller can't handle Unicode paths
def listdir(path):
    try:
        rep = os.listdir(path)
    except OSError as e:
        # Handle permission denied case
        if e.errno == 13:
            rep = []
        else:
            raise

    if type(path) == unicode:
        unicode_rep = []
        for s in rep:
            try:
                if type(s) is str:
                    value = s.decode('UTF-8')
                    unicode_rep.append(value)
                else:
                    unicode_rep.append(s)
            except UnicodeDecodeError:
                # If we can't decode a file, we omit it from the output and move on.
                logging.warn('Could not decode the path of a file:', exc_info=True)

        rep = unicode_rep

    return rep
