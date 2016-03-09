# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

from setuptools import setup
import platform

windows_deps = ['wmi'] if platform.system() == 'Windows' else []
linux_deps = ['netifaces'] if platform.system() == 'Linux' else []

if __name__ == '__main__':

    setup(
        name = "renderfarm_commons",
        version = "0.0.1",
        author = "Matthieu Riviere",
        author_email = "mriviere@luna-technology.com",
        description = "Some common packages used across the renderfarm",
        packages=[
                  'renderfarm_commons',
                  'renderfarm_commons.cache_client',
                  'renderfarm_commons.cluster_init',
                  'renderfarm_commons.fileserver',
                  'renderfarm_commons.job_runner',
                  'renderfarm_commons.job_submitter',
                  'renderfarm_commons.protocols',
                  'renderfarm_commons.stream_stats',
        ],
        install_requires= [
            'protobuf==2.5.0',
            'redis',
            'luna_commons',
            'requests',

            # Requirements for the evented cacheclient
            'Twisted>=14.0.2',
            'pyOpenSSL',
            'service-identity',
        ] + windows_deps + linux_deps,
        test_requires=[
            'nose',
        ]
    )