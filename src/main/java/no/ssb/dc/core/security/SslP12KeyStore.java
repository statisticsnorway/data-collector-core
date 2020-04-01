package no.ssb.dc.core.security;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;

class SslP12KeyStore implements SslKeyStore {

    private final CertificateBundle certificateBundle;

    public SslP12KeyStore(CertificateBundle certificateBundle) {
        Objects.requireNonNull(certificateBundle);
        Objects.requireNonNull(certificateBundle.archiveCert);
        this.certificateBundle = certificateBundle;
    }

    @Override
    public CertificateContext buildSSLContext() {
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            KeyStore keyStore = KeyStore.getInstance("PKCS12");

            try (ByteArrayInputStream bais = new ByteArrayInputStream(certificateBundle.archiveCert)) {
                keyStore.load(bais, certificateBundle.passphrase);
            }
            keyManagerFactory.init(keyStore, certificateBundle.passphrase);

            String alias = keyStore.aliases().nextElement();
            Key key = keyStore.getKey(alias, certificateBundle.passphrase);
            KeyPair keyPair = null;
            if (key instanceof PrivateKey) {
                Certificate cert = keyStore.getCertificate(alias);
                PublicKey publicKey = cert.getPublicKey();
                keyPair = new KeyPair(publicKey, (PrivateKey) key);
            }
            if (keyPair == null) {
                throw new IllegalStateException("Could not obtain PublicKey for bundle: " + certificateBundle.secretPropertiesPath.getParent());
            }

            X509Certificate cert = (X509Certificate) keyStore.getCertificate(alias);
            TrustManager[] trustManagers = {new SslPEMKeyStore.BusinessSSLTrustManager(cert)};

            SSLContext context = SSLContext.getInstance("TLSv1.2");
            context.init(
                    keyManagerFactory.getKeyManagers(),
                    trustManagers,
                    new SecureRandom()
            );

            return new CertificateContext(context, (X509TrustManager) trustManagers[0]);

        } catch (IOException | NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | KeyManagementException | CertificateException e) {
            throw new RuntimeException(e);
        }
    }

}
