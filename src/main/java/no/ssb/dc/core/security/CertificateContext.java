package no.ssb.dc.core.security;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.security.KeyPair;

public class CertificateContext {

    final SSLContext sslContext;
    final X509TrustManager trustManager;
    final KeyPair keyPair;

    public CertificateContext(SSLContext sslContext, X509TrustManager trustManager, KeyPair keyPair) {
        this.sslContext = sslContext;
        this.trustManager = trustManager;
        this.keyPair = keyPair;
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    public X509TrustManager trustManager() {
        return trustManager;
    }

    public KeyPair keyPair() {
        return keyPair;
    }
}
