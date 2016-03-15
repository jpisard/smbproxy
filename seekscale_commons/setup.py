# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

from setuptools import setup
import platform

windows_deps = ['wmi'] if platform.system() == 'Windows' else []
linux_deps = ['netifaces'] if platform.system() == 'Linux' else []

if __name__ == '__main__':

    setup(
        name = "seekscale_commons",
        version = "0.0.1",
        author = "Matthieu Riviere",
        author_email = "mriviere@luna-technology.com",
        description = "Some common packages used across the renderfarm",
        packages=[
                  'seekscale_commons',
                  'seekscale_commons.cache_client',
                  'seekscale_commons.stream_stats',
        ],
        install_requires= [
            'protobuf==2.5.0',
            'redis',
            'requests',
            'six',

            # Requirements for the evented cacheclient
            'Twisted>=14.0.2',
            'pyOpenSSL',
            'service-identity',
        ] + windows_deps + linux_deps,
        test_requires=[
            'nose',
        ]
    )