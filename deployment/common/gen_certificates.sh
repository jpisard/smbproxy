#!/usr/bin/env bash

## This script generates a ca, a ca.crt file, an entrypoint.{key,crt} pair and a gateway.{key,crt} pair.
## ca.crt + <server>.{key,crt} belong on <server>, in /etc/seekscale/keys


# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt -subj "/C=FR/L=Paris/O=Seekscale/CN=ca.entrypoint.seekscale.com"

# Generate server credentials
openssl genrsa -out entrypoint.key 4096
openssl req -new -key entrypoint.key -out entrypoint.csr -subj "/C=FR/L=Paris/O=Seekscale/CN=entrypoint.seekscale.com"
openssl x509 -req -days 365 -in entrypoint.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out entrypoint.crt

# Generate client credentials
openssl genrsa -out gateway.key 4096
openssl req -new -key gateway.key -out gateway.csr -subj "/C=FR/L=Paris/O=Seekscale/CN=gateway.seekscale.com"
openssl x509 -req -days 365 -in gateway.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out gateway.crt
