# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import logging
import unittest
import os
import shutil
import luna_commons
import random
import platform

import six


class TestExecCommand(unittest.TestCase):
    # TODO
    pass


class TestDownload(unittest.TestCase):
    # TODO
    pass


class TestSha256Sum(unittest.TestCase):
    def setUp(self):
        self.f = os.path.join(os.path.dirname(__file__), 'a_test_file.txt')
        self.f_contents = """This is a test file, that is used to test some functions.
Yes, really. That's it."""

        with open(self.f, 'w') as fh:
            fh.write(self.f_contents)

        self.f_sha256sum = "8278277d981279d390fb24168331fb31c7fc80152a8ba75c0c22c6a7930eb1e7"

    def tearDown(self):
        try:
            os.remove(self.f)
        except:
            pass

    def test_sha256sum_file(self):
        self.assertEqual(luna_commons.sha256sum(self.f), self.f_sha256sum)

    def test_sha256sum_file_absent(self):
        f = os.path.join(os.path.dirname(__file__), 'a_test_file_that_does_not_exist.txt')
        self.assertEqual(luna_commons.sha256sum(f), None)

    def test_sha256sum_fd(self):
        with open(self.f, 'rb') as fh:
            self.assertEqual(luna_commons.sha256sum_fd(fh), self.f_sha256sum)

    def test_sha256sum_fd_null(self):
        self.assertEqual(luna_commons.sha256sum_fd(None), None)


class TestCreateDir(unittest.TestCase):
    BASEDIRNAME = 'test_create_dir'

    def setUp(self):
        try:
            os.makedirs(self.BASEDIRNAME)
        except:
            pass

    def tearDown(self):
        try:
            shutil.rmtree(self.BASEDIRNAME)
        except:
            pass

    def test_create_dir(self):
        p = os.path.join(self.BASEDIRNAME, luna_commons.random_num_string(15))

        luna_commons.create_dir(p)

        self.assertTrue(os.path.exists(p))
        self.assertTrue(os.path.isdir(p))

    def test_create_subdir(self):
        p = os.path.join(
            self.BASEDIRNAME,
            luna_commons.random_num_string(15),
            luna_commons.random_num_string(15)
        )

        luna_commons.create_dir(p)

        self.assertTrue(os.path.exists(p))
        self.assertTrue(os.path.isdir(p))

    def test_create_existing_dir(self):
        p = os.path.join(
            self.BASEDIRNAME,
            luna_commons.random_num_string(15)
        )

        luna_commons.create_dir(p)
        luna_commons.create_dir(p)

        self.assertTrue(os.path.exists(p))
        self.assertTrue(os.path.isdir(p))

    def test_create_dir_root(self):
        """Tests that create_dir works on paths like "C:\\" when the drive exists"""
        if platform.system() == 'Windows':
            p = "C:\\"

            luna_commons.create_dir(p)

            self.assertTrue(os.path.exists(p))

    def test_create_dir_root_nonexisting(self):
        """Test that we don't return ok on paths like "G:\\" when the drive doesn't exist"""
        if platform.system() == 'Windows':
            p = "G:\\"

            self.assertFalse(os.path.exists(p))
            self.assertRaises(WindowsError, luna_commons.create_dir, p)


class TestListTree(unittest.TestCase):
    BASEDIRNAME = 'test_create_dir'

    def setUp(self):
        luna_commons.create_dir(self.BASEDIRNAME)

        self.p1 = os.path.join(self.BASEDIRNAME, luna_commons.random_num_string())
        self.p2 = os.path.join(self.BASEDIRNAME, luna_commons.random_num_string())
        self.p3 = os.path.join(self.BASEDIRNAME, luna_commons.random_num_string())

        self.p11 = os.path.join(self.p1, luna_commons.random_num_string())
        self.p21 = os.path.join(self.p2, luna_commons.random_num_string())
        self.p22 = os.path.join(self.p2, luna_commons.random_num_string())

        luna_commons.create_dir(self.p1)
        luna_commons.create_dir(self.p2)
        luna_commons.create_dir(self.p3)

        with open(self.p11, 'wb') as _:
            pass

        with open(self.p21, 'wb') as _:
            pass

        with open(self.p22, 'wb') as _:
            pass

    def tearDown(self):
        try:
            shutil.rmtree(self.BASEDIRNAME)
        except:
            pass

    def test_list_tree(self):
        l = luna_commons.list_tree(self.BASEDIRNAME)

        self.assertEquals(
            set(l),
            set([
                self.p11,
                self.p21,
                self.p22,
            ]))

    def test_list_tree_exclude_paths(self):
        l = luna_commons.list_tree(self.BASEDIRNAME, exclude_paths=[self.p1])

        self.assertEquals(
            set(l),
            set([
                self.p21,
                self.p22,
            ])
        )

    def test_list_tree_empty_dirs(self):
        l = luna_commons.list_tree(self.BASEDIRNAME, create_empty_dirs=True)

        self.assertEquals(
            set(l),
            set([
                self.p11,
                self.p21,
                self.p22,
                os.path.join(self.BASEDIRNAME, '.empty'),
                os.path.join(self.p1, '.empty'),
                os.path.join(self.p2, '.empty'),
                os.path.join(self.p3, '.empty'),
            ])
        )

    def test_list_tree_empty_dirs_exclude_paths(self):
        l = luna_commons.list_tree(self.BASEDIRNAME, exclude_paths=[self.p1], create_empty_dirs=True)

        self.assertEquals(
            set(l),
            set([
                self.p21,
                self.p22,
                os.path.join(self.BASEDIRNAME, '.empty'),
                os.path.join(self.p2, '.empty'),
                os.path.join(self.p3, '.empty'),
            ])
        )

    def test_list_nonexistant(self):
        l = luna_commons.list_tree('/nonexistent')
        self.assertEquals(l, [])

    def test_list_file(self):
        l = luna_commons.list_tree(self.p11)

        self.assertEquals(
            l,
            [self.p11]
        )

    def test_list_file_empty_dirs(self):
        l = luna_commons.list_tree(self.p11, create_empty_dirs=True)

        self.assertEquals(
            l,
            [self.p11]
        )

    def test_list_file_exclude_paths(self):
        l = luna_commons.list_tree(self.p11, exclude_paths=[self.p1])

        self.assertEquals(
            l,
            []
        )


class TestAscii36Encode(unittest.TestCase):
    def test_ascii36encode0(self):
        v = 0
        length = 3
        result = "000"
        self.assertEqual(luna_commons.ascii36encode(v, length), result)

    def test_ascii36Encode5000(self):
        v = 5000
        length = 3
        result = "WU3"
        self.assertEqual(luna_commons.ascii36encode(v, length), result)


class TestAscii36Decode(unittest.TestCase):
    def test_ascii36decode0(self):
        v = 0
        length = 3
        s = luna_commons.ascii36encode(v, length)

        self.assertEquals(luna_commons.ascii36decode(s), v)

    def test_ascii36decode5000(self):
        v = 0
        length = 3
        s = luna_commons.ascii36encode(v, length)

        self.assertEquals(luna_commons.ascii36decode(s), v)

    def test_ascii36brute(self):
        length = 20
        for i in range(5000):
            v = random.randint(0, 36**length)
            s = luna_commons.ascii36encode(v, length)

            self.assertEquals(luna_commons.ascii36decode(s), v)


class TestRandomNumString(unittest.TestCase):
    def test_random_num_string(self):
        l = random.randint(0, 9999)
        s = luna_commons.random_num_string(l)

        self.assertEqual(type(s), str)
        self.assertEqual(len(s), l)

    def test_random_num_string_unspec_length(self):

        s = luna_commons.random_num_string()

        self.assertEqual(type(s), str)
        self.assertTrue(len(s) > 0)


class TestRandomUnicodeString(unittest.TestCase):
    def test_random_unicode_string(self):
        l = random.randint(0, 999)
        s = luna_commons.random_unicode_string(l)

        self.assertEqual(type(s), six.text_type)
        self.assertEqual(len(s), l)

    def test_random_unicode_string_unspec_length(self):
        s = luna_commons.random_unicode_string()

        self.assertEqual(type(s), six.text_type)
        self.assertTrue(len(s) > 0)


class TestJsonLogger(unittest.TestCase):
    def test_json_logger(self):
        luna_commons.setup_logging(json=True)

        logging.debug('Flop', extra={'a': 1, 'b': self})