#!/bin/bash


redis-cli flushall
rm -rf /home/data/file_cache
rm -rf /home/data/smbshares/*/*/*
