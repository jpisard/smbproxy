# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

from setuptools import setup

if __name__ == '__main__':

    setup(
        name="luna_commons",
        version="0.0.1",
        author="Matthieu Riviere",
        author_email="mriviere@luna-technology.com",
        description="Various very commonly used functions and tools",
        packages=[
                'luna_commons',
                'luna_commons.task_queue',
        ],
        install_requires=[
            'requests',
            'redis',
            'six',
        ],
    )
