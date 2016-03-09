# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

if __name__ == '__main__':

    setup(
        name="smbproxy",
        version="4.0.3",
        author="Matthieu Riviere",
        author_email="mriviere@luna-technology.com",
        description="A SMB2 interception proxy and HTTP-bridge",
        license="Closed source",
        keywords="",
        py_modules=['__main__'],
        packages=[
                  'metadata_proxy',
                  'nmb',
                  'smb',
                  'smb.utils',
                  'smbproxy4',
        ],
        long_description=read('README'),
        classifiers=[
        ],
        package_data={
        },
        install_requires=[
            'twisted',
            'requests',
            'pyasn1',
            'redis',
            'luna_commons',
            'renderfarm_commons',
            'treq',
            'structlog',
            'pyyaml',
            'psycopg2',
            'statsd',
            'tornado',
        ],
    )
