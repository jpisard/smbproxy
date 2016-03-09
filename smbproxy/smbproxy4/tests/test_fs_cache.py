# coding: utf-8

# Copyright Luna Technology 2016
# Matthieu Riviere <mriviere@luna-technology.com>

from unittest import TestCase

from smbproxy4.fs_cache import FSCacheFileMetadata


class TestFSCacheFileMetadata(TestCase):
    def setUp(self):
        default_file_metadata = {
            'exists': True,
            'metadata': {
                'isfile': True
            }
        }

        default_dir_metadata = {
            'exists': True,
            'metadata': {
                'isdir': True
            }
        }

        self.file_metadata = FSCacheFileMetadata('\\\\HOST\\SHARE', 'my\\path', default_file_metadata, None)
        self.dir_metadata = FSCacheFileMetadata('\\\\HOST\\SHARE', 'my', default_dir_metadata, None)
        self.none_file_metadata = FSCacheFileMetadata('\\\\HOST\\SHARE', 'my\\path', None, None)

    def test_exists(self):
        self.file_metadata.metadata['exists'] = True
        assert self.file_metadata.exists() is True

        assert self.none_file_metadata.exists() is False

    def test_is_file(self):
        assert self.file_metadata.is_file() is True
        assert self.dir_metadata.is_file() is False
        assert self.none_file_metadata.is_file() is False

    def test_is_dir(self):
        assert self.file_metadata.is_dir() is False
        assert self.dir_metadata.is_dir() is True
        assert self.none_file_metadata.is_dir() is False