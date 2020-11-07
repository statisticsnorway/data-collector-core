package no.ssb.dc.core.security;

interface SslKeyStore {

    CertificateContext buildSSLContext();

}
