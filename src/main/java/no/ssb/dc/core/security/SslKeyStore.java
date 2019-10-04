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
import java.io.FileReader;
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

public class SslKeyStore {

    private final String publicCertFile;
    private final String privateKeyFile;
    private final String protocol;
    private final String passphrase;
    private final JcaX509CertificateConverter certificateConverter;
    private final JcaPEMKeyConverter privateKeyconverter;

    private SslKeyStore(String publicCertFile, String privateKeyFile, String passphrase, String protocol) {
        this.publicCertFile = publicCertFile;
        this.privateKeyFile = privateKeyFile;
        this.passphrase = passphrase;
        this.protocol = protocol;
        Security.addProvider(new BouncyCastleProvider());
        certificateConverter = new JcaX509CertificateConverter().setProvider("BC");
        privateKeyconverter = new JcaPEMKeyConverter().setProvider("BC");
    }

    private X509Certificate loadPEMCertificate(String certFile) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(new FileReader(certFile))) {
            X509CertificateHolder certHolder = (X509CertificateHolder) pemParser.readObject();
            return certificateConverter.getCertificate(certHolder);
        }
    }

    private PrivateKey loadPrivateKey() throws IOException {
        try (PEMParser pemParser = new PEMParser(new FileReader(privateKeyFile))) {
            final Object pemObject = pemParser.readObject();
            final KeyPair kp;
            if (pemObject instanceof PEMEncryptedKeyPair) {
                // Encrypted key - we will use provided password
                final PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(passphrase.toCharArray());
                kp = privateKeyconverter.getKeyPair(((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decProv));

            } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Encrypted key - we will use provided password
                try {
                    final PKCS8EncryptedPrivateKeyInfo encryptedInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                    final InputDecryptorProvider provider = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
                    final PrivateKeyInfo privateKeyInfo = encryptedInfo.decryptPrivateKeyInfo(provider);
                    return privateKeyconverter.getPrivateKey(privateKeyInfo);
                } catch (PKCSException | OperatorCreationException e) {
                    throw new IOException("Unable to decrypt private key!", e);
                }

            } else if (pemObject instanceof PrivateKeyInfo) {
                return privateKeyconverter.getPrivateKey((PrivateKeyInfo) pemObject);

            } else {
                // Unencrypted key - no password needed
                kp = privateKeyconverter.getKeyPair((PEMKeyPair) pemObject);
            }

            return kp.getPrivate();
        }
    }

    private SSLContext buildSSLContext() {
        try {
            //  Load client certificate
            X509Certificate cert = loadPEMCertificate(publicCertFile);

            // Load client private key
            PrivateKey privateKey = loadPrivateKey();

            // Client key and certificates are sent to server so it can authenticate the client
            KeyStore clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            clientKeyStore.load(null, null);
            clientKeyStore.setCertificateEntry("certificate", cert);
            clientKeyStore.setKeyEntry("privateKey", privateKey, passphrase.toCharArray(), new Certificate[]{cert});

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, passphrase.toCharArray());

            // Create SSL socket factory
            SSLContext context = SSLContext.getInstance(protocol);
            context.init(keyManagerFactory.getKeyManagers(), new TrustManager[]{new BusinessSSLTrustManager(cert)}, new SecureRandom());

            return context;

        } catch (IOException | CertificateException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    public static SSLContext getSSLContext(String publicCertFile, String privateKeyFile, String passphrase, String protocol) {
        return new SslKeyStore(publicCertFile, privateKeyFile, passphrase, protocol).buildSSLContext();
    }

    // TODO: implement server and client cert verification with black-/whitelisting
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
