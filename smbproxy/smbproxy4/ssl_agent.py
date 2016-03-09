# coding: utf-8

# Copyright Luna Technology 2016
# Matthieu Riviere <mriviere@luna-technology.com>


from twisted.internet import reactor
from twisted.internet.ssl import Certificate, PrivateCertificate, optionsForClientTLS
from twisted.python.filepath import FilePath
from twisted.web.client import Agent
from twisted.web.client import BrowserLikePolicyForHTTPS, _requireSSL
from twisted.web.client import HTTPConnectionPool


class BrowserLikePolicyForHTTPSWithClientCertificate(BrowserLikePolicyForHTTPS):
    """Extend the default HTTPS certificate policy to send a given SSL certificate"""
    def __init__(self, trustRoot=None, clientCertificate=None):
        self._trustRoot = trustRoot
        self._clientCertificate = clientCertificate

    @_requireSSL
    def creatorForNetloc(self, hostname, port):
        return optionsForClientTLS(
            hostname.decode("ascii"),
            trustRoot=self._trustRoot,
            clientCertificate=self._clientCertificate)


def create_agent(ca_cert, client_cert, client_key):
    ca_certificate = Certificate.loadPEM(FilePath(ca_cert).getContent())
    client_certificate = PrivateCertificate.loadPEM(
        FilePath(client_cert).getContent() + b"\n" +
        FilePath(client_key).getContent())

    customPolicy = BrowserLikePolicyForHTTPSWithClientCertificate(
        trustRoot=ca_certificate,
        clientCertificate=client_certificate)

    pool = HTTPConnectionPool(reactor, persistent=True)
    agent = Agent(reactor, customPolicy, pool=pool)

    return agent
