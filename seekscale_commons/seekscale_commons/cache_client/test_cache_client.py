# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""In order to run, these tests need a local redis+ftp cache.
It will write lots of junk! Don't use against a production server!

These tests run on linux, for more convenience.
They must be ran as root (to be able to cleanup the ftp directory).
"""

import unittest
import shutil
import base64
import os
import redis
import logging
import random
import time
from threading import Thread
from nose.plugins.attrib import attr
import filecache_client3

from ..base import create_dir, random_num_string, sha256sum, setup_logging

BLANK_FILE = '/tmp/blank'

BASE_TESTS_DIR = '/tmp/CacheClientTest'


@attr(cat='integration')
class CacheClientTest(unittest.TestCase):

    def setUp(self):
        create_dir(BASE_TESTS_DIR)
        self.cacheclient = filecache_client3.CacheClient3()
        self.cacheclient.BLANK_FILE = BLANK_FILE

        self.redis = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

        # Create dummy files for testing:
        self.existing_file_path = os.path.join(BASE_TESTS_DIR, 'a_file')
        with open(self.existing_file_path, 'wb') as fh:
            for i in range(777):
                fh.write(str(random.randint(0, 9999)))

        self.existing_file_path2 = os.path.join(BASE_TESTS_DIR, 'a_file_2')
        with open(self.existing_file_path2, 'wb') as fh:
            for i in range(4237):
                fh.write(str(random.randint(0, 9999)))

        self.existing_file_path3 = os.path.join(BASE_TESTS_DIR, u'ééé')
        with open(self.existing_file_path3, 'wb') as fh:
            for i in range(600):
                fh.write(str(random.randint(0, 9999)))

        self.retrieved_file = os.path.join(BASE_TESTS_DIR, random_num_string(15))
        self.retrieved_file2 = os.path.join(BASE_TESTS_DIR, random_num_string(12))

    def tearDown(self):
        try:
            shutil.rmtree(BASE_TESTS_DIR)
        except Exception:
            pass

        self.cacheclient.clear()

    def test_key_from_file(self):
        # Non existing file, return a key for the default blank file
        blank_b64 = base64.b64encode(BLANK_FILE)

        bad_key = self.cacheclient.key_from_file(
            os.path.join(BASE_TESTS_DIR, 'nonexistent')
        )

        self.assertTrue(bad_key.startswith('renderfarm:file:%s:0' % blank_b64))

        # Existing file: returns something else
        some_key = self.cacheclient.key_from_file(self.existing_file_path)

        self.assertNotEqual(some_key, bad_key)

    def test_has_file(self):
        key = 'tralalalala'
        key2 = 'trululululu'
        self.redis.set(key, 'a_test_value')

        self.assertEqual(self.cacheclient.has_file(key), True)
        self.assertEqual(self.cacheclient.has_file(key2), False)

    def test_add_file(self):
        some_key = self.cacheclient.key_from_file(self.existing_file_path)
        sha = sha256sum(self.existing_file_path)

        self.cacheclient.add_file(some_key, self.existing_file_path)

        self.assertEquals(self.redis.get(some_key), sha)

        self.cacheclient.get_file(self.redis.get(some_key), self.retrieved_file)

        self.assertEquals(
            os.path.getsize(self.retrieved_file),
            os.path.getsize(self.existing_file_path)
        )
        self.assertEquals(
            sha256sum(self.retrieved_file),
            sha
        )

    def test_add_nonexistent_file(self):
        path = os.path.join(BASE_TESTS_DIR, 'nonexistent')
        some_key = self.cacheclient.key_from_file(BLANK_FILE)
        sha = sha256sum(BLANK_FILE)

        self.cacheclient.add_file(some_key, path)

        self.assertEquals(self.redis.get(some_key), sha)

        self.cacheclient.get_file(self.redis.get(some_key), self.retrieved_file)
        self.assertEquals(
            os.path.getsize(self.retrieved_file),
            0
        )

    def test_cache_file(self):
        r = self.cacheclient.cache_file(self.existing_file_path)

        # We want the file to be in the right place on the ftp
        self.cacheclient.get_file(r['sha256'], self.retrieved_file)

        self.assertTrue(
            os.path.getsize(self.retrieved_file) > 0
        )
        self.assertEquals(
            sha256sum(self.retrieved_file),
            sha256sum(self.existing_file_path)
        )
        self.assertEquals(
            r['sha256'],
            sha256sum(self.existing_file_path)
        )

    def test_cache_file_with_unicode_name(self):
        r = self.cacheclient.cache_file(self.existing_file_path3)

        self.cacheclient.get_file(r['sha256'], self.retrieved_file)
        self.assertTrue(
            os.path.getsize(self.retrieved_file) > 0
        )
        self.assertEqual(
            sha256sum(self.retrieved_file),
            sha256sum(self.existing_file_path3)
        )
        self.assertEqual(
            r['sha256'],
            sha256sum(self.existing_file_path3),
        )

    def test_multiple_cache_file(self):
        """Tests that we can put multiple files in the cache"""
        r1 = self.cacheclient.cache_file(self.existing_file_path)
        r2 = self.cacheclient.cache_file(self.existing_file_path2)

        self.cacheclient.get_file(r1['sha256'], self.retrieved_file)
        self.cacheclient.get_file(r2['sha256'], self.retrieved_file2)

        self.assertTrue(
            os.path.getsize(self.retrieved_file) > 0
        )
        self.assertTrue(
            os.path.getsize(self.retrieved_file2) > 0
        )

        self.assertEqual(
            sha256sum(self.retrieved_file),
            sha256sum(self.existing_file_path)
        )
        self.assertEqual(
            sha256sum(self.retrieved_file2),
            sha256sum(self.existing_file_path2)
        )

        self.assertEqual(
            r1['sha256'],
            sha256sum(self.existing_file_path)
        )
        self.assertEqual(
            r2['sha256'],
            sha256sum(self.existing_file_path2)
        )

    # FIXME: How do we test this with Swift ?
    # def test_repeat_cache_file(self):
    #     """Tests that the cache works, ie that a same file is cached only once"""
    #     r1 = self.cacheclient.cache_file(self.existing_file_path)
    #
    #     stored_path = os.path.join(FTP_ROOT, r1['sha256'])
    #     ctime = os.stat(stored_path).st_mtime
    #
    #     time.sleep(5)
    #
    #     r2 = self.cacheclient.cache_file(self.existing_file_path)
    #
    #     self.assertEqual(r1['sha256'], r2['sha256'])
    #
    #     ctime2 = os.stat(stored_path).st_mtime
    #     self.assertEqual(ctime, ctime2)

    # FIXME: How do we test this with swift ?
    # def test_multiple_identical_files(self):
    #     """Tests that the deduplication of files work, ie that identical files aren't uploaded multiple times"""
    #     copied_file = os.path.join(BASE_TESTS_DIR, 'a_duplicate_source_file')
    #     shutil.copyfile(self.existing_file_path, copied_file)
    #     # This is just a security, not a real test
    #     self.assertEquals(
    #         sha256sum(self.existing_file_path),
    #         sha256sum(copied_file)
    #     )
    #
    #     r1 = self.cacheclient.cache_file(self.existing_file_path)
    #     stored_path = os.path.join(FTP_ROOT, r1['sha256'])
    #     ctime = os.stat(stored_path).st_mtime
    #
    #     time.sleep(5)
    #
    #     r2 = self.cacheclient.cache_file(copied_file)
    #
    #     self.assertEquals(r1['sha256'], r2['sha256'])
    #
    #     ctime2 = os.stat(stored_path).st_mtime
    #     self.assertEqual(ctime, ctime2)

    def test_repeat_modified_cache_file(self):
        """Tests that if a file changes, the cache is invalidated"""
        r1 = self.cacheclient.cache_file(self.existing_file_path)
#        stored_path1 = os.path.join(FTP_ROOT, r1['sha256'])
        sha1 = sha256sum(self.existing_file_path)

        self.cacheclient.get_file(r1['sha256'], self.retrieved_file)

        with open(self.existing_file_path, 'wb') as fh:
            fh.write('Pwnd')

        r2 = self.cacheclient.cache_file(self.existing_file_path)
        self.assertNotEqual(r1['sha256'], r2['sha256'])

        sha2 = sha256sum(self.existing_file_path)
        self.cacheclient.get_file(r2['sha256'], self.retrieved_file2)

        self.assertEquals(
            sha256sum(self.retrieved_file),
            sha1
        )
        self.assertEquals(
            sha1,
            r1['sha256']
        )

        self.assertEquals(
            sha256sum(self.retrieved_file2),
            sha2
        )
        self.assertEquals(
            sha2,
            r2['sha256']
        )

    def test_repeat_modified_samesize_cache_file(self):
        """Tests that if a file changes and size doesn't change, the cache is invalidated"""
        r1 = self.cacheclient.cache_file(self.existing_file_path)
        self.cacheclient.get_file(r1['sha256'], self.retrieved_file)
        sha1 = sha256sum(self.existing_file_path)

        # Wait a bit. We don't ask the system to detect changes occuring in the same second
        time.sleep(5)
        with open(self.existing_file_path, 'rb') as fh:
            data = fh.read()

        # Replace a character in the middle
        l = len(data)
        data = data[:l/2] + 'Z' + data[(l/2)+1:]

        # Write the modified data
        with open(self.existing_file_path, 'wb') as fh:
            fh.write(data)

        r2 = self.cacheclient.cache_file(self.existing_file_path)
        self.assertNotEqual(r1['sha256'], r2['sha256'])

        self.cacheclient.get_file(r2['sha256'], self.retrieved_file2)
        sha2 = sha256sum(self.existing_file_path)

        self.assertEquals(
            sha256sum(self.retrieved_file),
            sha1
        )
        self.assertEquals(
            sha1,
            r1['sha256']
        )

        self.assertEquals(
            sha256sum(self.retrieved_file2),
            sha2
        )
        self.assertEquals(
            sha2,
            r2['sha256']
        )

    def test_simultaneous_cache_file(self):
        """Tests that the system doesn't break when trying to cache a file many times"""

        # Interestingly, the system seems to hold fine even with a high number of threads.
        # There is probably a bottleneck somewhere else
        concurrency = 50
        sha = sha256sum(self.existing_file_path)

        def send_file():
            self.cacheclient.cache_file(self.existing_file_path)

        threads = []
        for i in range(concurrency):
            t = Thread(target=send_file, args=())
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        self.cacheclient.get_file(sha, self.retrieved_file)

        self.assertEqual(
            sha256sum(self.retrieved_file),
            sha
        )

    def test_simultaneous_cache_file_copies(self):
        """Tests that the system doesn't break when trying to cache multiple copies of
        the same file at the same time"""
        # Fails at concurrency level 50, maybe below.
        concurrency = 50
        sha = sha256sum(self.existing_file_path)

        threads = []
        source_files = []
        for i in range(concurrency):
            p = os.path.join(BASE_TESTS_DIR, 'duplicated_source_file_%d' % i)
            source_files.append(p)
            shutil.copyfile(self.existing_file_path, p)

        def send_file(p2):
            self.cacheclient.cache_file(p2)

        for p in source_files:
            t = Thread(target=send_file, args=(p,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        self.cacheclient.get_file(sha, self.retrieved_file)

        self.assertEqual(
            sha256sum(self.retrieved_file),
            sha
        )

    def test_get_file(self):
        """Tests simple retrieval of a file"""
        r = self.cacheclient.cache_file(self.existing_file_path)
        sha = r['sha256']

        target_path = self.existing_file_path + '_2'

        self.cacheclient.get_file(sha, target_path)

        self.assertEquals(
            sha256sum(target_path),
            sha256sum(self.existing_file_path)
        )

    def test_get_file_in_nonexisting_directory(self):
        """Tests retrieval of a file into a directory that doesn't exist yet"""
        r = self.cacheclient.cache_file(self.existing_file_path)
        sha = r['sha256']

        target_path = os.path.join(BASE_TESTS_DIR, 'this_is_a_directory/this_is_a_file')

        self.cacheclient.get_file(sha, target_path)

        self.assertEqual(
            sha256sum(target_path),
            sha256sum(self.existing_file_path)
        )

    def test_get_file_to_unicode_path(self):
        """Tests that we can retrieve a file to a unicode path"""
        r = self.cacheclient.cache_file(self.existing_file_path)
        sha = r['sha256']

        target_path = os.path.join(BASE_TESTS_DIR, u'Wééééé')

        self.cacheclient.get_file(sha, target_path)

        self.assertEquals(
            sha256sum(target_path),
            sha256sum(self.existing_file_path)
        )

    def test_multiple_get_file(self):
        """Tests retrieval of two different files"""
        r1 = self.cacheclient.cache_file(self.existing_file_path)
        r2 = self.cacheclient.cache_file(self.existing_file_path2)
        sha1 = r1['sha256']
        sha2 = r2['sha256']
        target_path1 = os.path.join(BASE_TESTS_DIR, 'retrieved_file_1')
        target_path2 = os.path.join(BASE_TESTS_DIR, 'retrieved_file_2')

        self.cacheclient.get_file(sha1, target_path1)
        self.cacheclient.get_file(sha2, target_path2)

        self.assertEquals(
            sha256sum(target_path1),
            sha256sum(self.existing_file_path)
        )

        self.assertEquals(
            sha256sum(target_path2),
            sha256sum(self.existing_file_path2)
        )

    def test_simultaneous_get_file(self):
        """Tests simultaneous retrievals"""

        # A concurrency level of 10 seems to still be okay. 50 isn't.
        concurrency = 50

        target_sha = self.cacheclient.cache_file(self.existing_file_path)['sha256']

        def get_file(target_path):
            self.cacheclient.get_file(target_sha, target_path)

        threads = []
        target_paths = []

        for i in range(concurrency):
            target_paths.append(os.path.join(BASE_TESTS_DIR, 'retrieved_concurrent_file_%d' % i))

        for p in target_paths:
            t = Thread(target=get_file, args=(p,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        for p in target_paths:
            self.assertEquals(
                sha256sum(p),
                target_sha
            )


if __name__ == '__main__':
    setup_logging(level=logging.FATAL)
    unittest.main()
