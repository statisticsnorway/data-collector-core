package no.ssb.dc.core.security;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.CharArrayReader;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

class SslPEMKeyStore implements SslKeyStore {

    private final JcaX509CertificateConverter certificateConverter;
    private final JcaPEMKeyConverter privateKeyConverter;
    private final CertificateBundle certificateBundle;

    SslPEMKeyStore(CertificateBundle certificateBundle) {
        this.certificateBundle = certificateBundle;
        Security.addProvider(new BouncyCastleProvider());
        certificateConverter = new JcaX509CertificateConverter().setProvider("BC");
        privateKeyConverter = new JcaPEMKeyConverter().setProvider("BC");
    }

    private X509Certificate loadPEMCertificate(char[] certCharArray) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(new CharArrayReader(certCharArray))) {
            X509CertificateHolder certHolder = (X509CertificateHolder) pemParser.readObject();
            return certificateConverter.getCertificate(certHolder);
        }
    }

    private PrivateKey loadPrivateKey() throws IOException {
        try (PEMParser pemParser = new PEMParser(new CharArrayReader(certificateBundle.privateKey))) {
            final Object pemObject = pemParser.readObject();
            final KeyPair kp;
            if (pemObject instanceof PEMEncryptedKeyPair) {
                // Encrypted key - we will use provided password
                final PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(certificateBundle.passphrase);
                kp = privateKeyConverter.getKeyPair(((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decProv));

            } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Encrypted key - we will use provided password
                try {
                    final PKCS8EncryptedPrivateKeyInfo encryptedInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                    final InputDecryptorProvider provider = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(certificateBundle.passphrase);
                    final PrivateKeyInfo privateKeyInfo = encryptedInfo.decryptPrivateKeyInfo(provider);
                    return privateKeyConverter.getPrivateKey(privateKeyInfo);
                } catch (PKCSException | OperatorCreationException e) {
                    throw new IOException("Unable to decrypt private key!", e);
                }

            } else if (pemObject instanceof PrivateKeyInfo) {
                return privateKeyConverter.getPrivateKey((PrivateKeyInfo) pemObject);

            } else {
                // Unencrypted key - no password needed
                kp = privateKeyConverter.getKeyPair((PEMKeyPair) pemObject);
            }

            return kp.getPrivate();
        }
    }

    @Override
    public CertificateContext buildSSLContext() {
        try {
            //  Load client certificate
            X509Certificate cert = loadPEMCertificate(certificateBundle.publicCert);

            // Load client private key
            PrivateKey privateKey = loadPrivateKey();

            // Client key and certificates are sent to server so it can authenticate the client
            KeyStore clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            clientKeyStore.load(null, null);
            clientKeyStore.setCertificateEntry("certificate", cert);
            clientKeyStore.setKeyEntry("privateKey", privateKey, certificateBundle.passphrase, new Certificate[]{cert});

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, certificateBundle.passphrase);

            // Create SSL socket factory
            SSLContext context = SSLContext.getInstance(certificateBundle.protocol);
            TrustManager[] trustManagers = {new BusinessSSLTrustManager(cert)};
            context.init(keyManagerFactory.getKeyManagers(), trustManagers, new SecureRandom());

            // overwrite secure tokens
            certificateBundle.clear();

            return new CertificateContext(context, (X509TrustManager) trustManagers[0]);

        } catch (IOException | CertificateException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    static class BusinessSSLTrustManager implements X509TrustManager {

        private final X509Certificate[] acceptedIssuers;

        BusinessSSLTrustManager(X509Certificate... acceptedIssuers) {
            if (acceptedIssuers == null || acceptedIssuers.length == 0) {
                throw new IllegalStateException("Business SSL Certificate must be applied!");
            }
            this.acceptedIssuers = acceptedIssuers;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return acceptedIssuers;
        }
    }
}
