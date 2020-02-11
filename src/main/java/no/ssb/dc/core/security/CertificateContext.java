package no.ssb.dc.core.security;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

public class CertificateContext {

    final SSLContext sslContext;
    final X509TrustManager trustManager;

    public CertificateContext(SSLContext sslContext, X509TrustManager trustManager) {
        this.sslContext = sslContext;
        this.trustManager = trustManager;
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    public X509TrustManager trustManager() {
        return trustManager;
    }
}
