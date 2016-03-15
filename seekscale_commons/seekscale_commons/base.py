# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""Luna Python Commons
This is a small set of very commonly used functions"""

import logging
import subprocess
import locale
import hashlib
import os
import socket
import time
import string
import random

import requests

from . import jsonlogger

LOGGING_FORMAT = "%(levelname)s %(asctime)-15s %(name)s:%(pathname)s:%(lineno)s %(message)s"

logger = logging.getLogger(__name__)


# Prevents things to fail on linux because WindowsError isn't defined there.
# Trick stolen from shutil.
try:
    WindowsError
except NameError:
    WindowsError = None


def setup_logging(**kwargs):
    """Setup logging with sane defaults"""
    enable_json = kwargs.get('json', False)
    loglevel = kwargs.get('level', logging.INFO)

    if enable_json:
        jsonlogger.setup_logging(level=loglevel)
    else:
        default_args = {
            'format': LOGGING_FORMAT,
            'level': logging.INFO,
        }
        default_args.update(kwargs)
        logging.basicConfig(**default_args)


def exec_command(command, **kwargs):
    """Execute a shell command.
    Any kwargs are passed to subprocess.Popen()
    Returns (return_code, stdout, stderr)"""

    # TODO: Rationalise env handling
    # TODO: Rationalise shell=True vs shell=False

    logging.debug(command)
    default_args = {
        'stdout': subprocess.PIPE,
        'stderr': subprocess.PIPE,
        'shell': False
    }
    default_args.update(kwargs)
    p = subprocess.Popen(command, **default_args)
    out, err = p.communicate()
    encoding = locale.getpreferredencoding()

    retcode = p.returncode
    retout = out.decode(encoding)
    reterr = err.decode(encoding)

    logger.debug(retcode)
    logger.debug(retout)
    logger.debug(reterr)

    return retcode, retout, reterr


def download(url, path, timeout=None):
    """Downloads a file via HTTP. Returns True in case of success"""

    if os.path.exists(path):
        logger.error('Path %s already taken. Assuming file has already been downloaded' % path)
        return True

    r = requests.get(url, stream=True, timeout=timeout)

    if r.status_code != 200:
        logger.error('Could not fetch %s (error %d)' % (url, r.status_code))
        return False

    try:
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)
    except Exception:
        logger.exception('Could not save to %s' % path)
        return False

    return True


def sha256sum(path):
    """Returns the SHA256 checksum of a file, or None if the file can't be processed"""
    try:
        sha256 = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(128*sha256.block_size), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        logger.warning('Could not compute sha256sum for %s (%s)' % (path, e))
        return None


def sha256sum_fd(fd):
    """Computes the SHA256 checksum of an open file, or None if it can't be processed"""
    try:
        fd.seek(0)
        sha256 = hashlib.sha256()
        for chunk in iter(lambda: fd.read(128*sha256.block_size), b''):
            sha256.update(chunk)
        fd.seek(0)
        return sha256.hexdigest()
    except Exception:
        logger.warning('Could not compute sha256sum for file descriptor')
        return None


def create_dir(path):
    """Ensure a directory exists. Creates it otherwise.
    This catches all sorts of "Directory already exist" exceptions,
    and lets the others pass through unchanged."""
    try:
        os.makedirs(path)
    except IOError as e:
        if e.errno == 17:
            # Already exists. Ok for us.
            pass
        else:
            raise e
    except WindowsError as e:
        if e.winerror == 183:
            # Already exists
            pass
        elif e.winerror == 5:
            # Access denied.
            # We get this error when we do os.makedirs("C:\\") (it does *not* happen for "C:")
            # No idea why, but fixing it anyway.
            # We get the error whether the drive exists or not, so we need to test that ourselves
            drive, subpath = os.path.splitdrive(path)
            if subpath == "\\" and os.path.exists(drive):
                pass
            else:
                raise e
        else:
            raise e
    except OSError as e:
        if e.errno == 17:
            # Already exists. Ok for us.
            pass
        else:
            raise e


def list_tree(rootdir, exclude_paths=None, create_empty_dirs=False):
    """Lists the full hierarchy of files (not the directories) under rootdir.
    Returns a list of paths.
    exclude_paths is a list of paths to exclude from the search (default: [])
    create_empty_dirs adds '.empty' files to the generated list (useful to replicate
      a hierarchy on a distant server)"""
    if exclude_paths is None:
        exclude_paths = []

    def issubpath(parent, path):
        """Tests whether path is a subpath of parent"""
        def fixpath(p):
            return os.path.normpath(p) + os.sep
        return fixpath(path).startswith(fixpath(parent))

    if not os.path.exists(rootdir):
        return []

    if os.path.isfile(rootdir):
        for excluded_path in exclude_paths:
            if issubpath(excluded_path, rootdir):
                return []
        return [rootdir]

    if os.path.isdir(rootdir):
        ret = []
        try:
            for (d, subdirs, files) in os.walk(rootdir):
                # Check that directory isn't in the excluded ones
                process = True
                for excluded_path in exclude_paths:
                    if issubpath(excluded_path, d):
                        process = False
                        break

                if process:
                    for f in files:
                        ret.append(os.path.join(d, f))
                    if create_empty_dirs is True:
                        ret.append(os.path.join(d, '.empty'))
    #                    for subdir in subdirs:
    #                        ret.append(os.path.join(d, subdir, '.empty'))
        except Exception:
            pass
        return ret
    else:
        return []


def wait_for_open_port(host, port):
    """Tries to connect to a TCP port until it opens"""
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.close()
            break
        except Exception as e:
            logger.info("Port %d on %s not ready: %s" % (port, host, e))
            time.sleep(5)


def ascii36encode(value, output_length):
    """Encodes an integer value in base 36, the base being 0123...9ABCDEF...Z
    The resulting value can be used as a hostname. It will have a length of
    output_length"""
    letters = string.digits + string.ascii_uppercase
    num_letters = len(letters)

    values = [
        letters[(value//(num_letters**x)) % num_letters]
        for x in range(output_length)
    ]

    return ''.join(values)


def ascii36decode(value):
    letters = string.digits + string.ascii_uppercase
    num_letters = len(letters)

    mul = 1
    s = 0

    for letter in value:
        s += letters.find(letter) * mul

        mul *= num_letters

    return s


def random_num_string(length=None):
    if length is None:
        length = random.randint(1, 100)

    return ''.join([str(random.randint(0, 9)) for _ in range(length)])


def random_unicode_string(length=None):
    if length is None:
        length = random.randint(1, 100)

    korean_characters = u'출발드림팀걸그룹버블슈트챔피언전'
    japanese_characters = u'私は寿司がいいです'
    russian_characters = u'Вдругпу́тни'
    french_characters = u'àéèêëîï'

    all_characters = korean_characters + japanese_characters + russian_characters + french_characters
    cs_size = len(all_characters)

    return u''.join([all_characters[random.randint(0, cs_size-1)] for _ in range(length)])
