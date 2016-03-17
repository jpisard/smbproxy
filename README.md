Smbproxy
========

Smbproxy is a caching system designed to provide cross-datacenter access to shared files. The primary use case is this one:

  * You have a Windows or Linux SMB file server, in datacenter 1
  * You want to make the files on this fileserver available (for example, for render nodes) in another datacenter (datacenter 2), possibly far away.
  * You have a decent, but not extremely fast/low latency connection (say, ~100-200Mb/s with possibly 100 or 200ms of latency), so connecting the fileserver through a tunnel results in very low performance.

What the smbproxy does is create a proxy fileserver, in datacenter 2, that will retrieve files from the "real" fileserver on the fly, and cache them, in order to save bandwidth between the two datacenters.


### Use cases

  * Efficiently mount your fileserver in the remote datacenter with caching and on the fly asset syncing
  * Seamless distributed asset syncing, unified namespace


Table of Contents
=================

1. [Getting Started](#getting-started)
2. [Documentation](#documentation)
3. [Licensing](#licensing)
4. [Support](#support)



Getting Started
===============

To get started, you will need:

  * In datacenter 1, a machine or virtual machine running Ubuntu 14.04. For best performance, allocate at least 2 cores, 2GB of RAM, and at least 15GB of disk space. In the rest of the documentation, we will call this machine the **gateway**.

  * In datacenter 2, a machine or virtual machine running Ubuntu 14.04. Since this machine will keep all cached files, more disk space is always good. 500G or 1TB are good values. Depending on the amount of load you will put on the system, it could require up to 8G of RAM and 4 cores. In the rest of the documentation, we will call this machine the **entrypoint**

  * Both machines should have an ip that is directly reachable by the other.


### Installing packages

Clone this repository on both the gateway and the entrypoint.
On the entrypoint, run:

```
cd deployment/entrypoint
bash setup.sh
```

On the gateway, run:

```
cd deployment/gateway
bash setup.sh
```

This initial setup will install and start supporting services, and prepare the environment.


### Generating certificates

On any machine, run the script deployment/common/gen_certificates.sh
It will generate the ssl certificates that will be used to secure the connection between the gateway and the entrypoint.

Copy these certificates:

 * gateway.crt, gateway.key and ca.crt into /etc/seekscale/certs on the gateway
 * entrypoint.crt, entrypoint.key and ca.crt into /etc/seekscale/certs on the entrypoint

### Configuring

There is a single configuration file on each machine. On the gateway, it is located at */etc/seekscale/gateway.yaml* and on the entrypoint, it is located at */etc/smbproxy4.conf*

Update these two files. The most important setting is *remote_host*. It tells each server where it can find the other. So, on the entrypoint, this should be set to the gateway, and on the gateway, this should be set to the entrypoint.

When you are done, run

```
seekscale-reconfigure
```

to apply the configuration, and the system should be operational.


### Checking

From a machine in datacenter 2, try to connect to \\\\*entrypoint_ip*. You should see the same shares and the same files as present in the original fileserver.

It is important to note that until you actually try to open them, the files aren't transferred towards datacenter 2. They only appear to be present.
When you try to open a file, you will notice that, the first time, the file transfer can appear to be frozen. This is a sign that the file is being copied from the real fileserver to the entrypoint.
Once the transfer is done, the file will open as usual, and if you access the file again, it will be served directly from the cache. If the file is modified in datacenter 1, the cache will detect it and re-import the file.
If the file is modified in datacenter 2, the changes will be transmitted back to the real fileserver.


Documentation
=============

[SmbproxyCodeOverview.pdf](https://github.com/Seekscale/smbproxy/blob/master/doc/SmbproxyCodeOverview.pdf) presents a lot of further context on the way smbproxy is designed to be used and on the code internals


Licensing
=========
Smbproxy is licensed under the GNU GPLv2 or later. See [LICENSE](https://github.com/seekscale/smbproxy/blob/master/LICENSE) for the full license text.

Smbproxy contains modified code from [pysmb](https://github.com/miketeo/pysmb), used in accord with its [license](https://github.com/miketeo/pysmb/blob/master/LICENSE).

For a commercial license, if you want to embed smbproxy in a commercial product, [contact us](mailto:fruty@seekscale.com).


Support
=======
Commercial support is available, please [contact us](mailto:fruty@seekscale.com) for more information.


