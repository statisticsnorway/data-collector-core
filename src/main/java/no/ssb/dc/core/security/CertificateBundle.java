package no.ssb.dc.core.security;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

class CertificateBundle {
    final Path secretPropertiesPath;
    final char[] passphrase;
    final char[] privateKey;  // base64 pem certificate
    final char[] publicCert;  // base64 pem certificate
    final byte[] archiveCert; // binary certificate archive
    final String protocol = "TLSv1.2";

    private CertificateBundle(Path secretPropertiesPath, char[] passphrase, char[] privateKey, char[] publicCert) {
        this.secretPropertiesPath = secretPropertiesPath;
        this.passphrase = passphrase;
        this.privateKey = privateKey;
        this.publicCert = publicCert;
        this.archiveCert = null;
    }

    private CertificateBundle(Path secretPropertiesPath, char[] passphrase, byte[] archiveCert) {
        this.secretPropertiesPath = secretPropertiesPath;
        this.passphrase = passphrase;
        this.privateKey = null;
        this.publicCert = null;
        this.archiveCert = archiveCert;
    }

    boolean isArchive() {
        return privateKey == null && publicCert == null && archiveCert != null;
    }

    void clear() {
        emptyCharArray(passphrase);
        emptyCharArray(privateKey);
        emptyCharArray(publicCert);
        emptyByteArray(archiveCert);
    }

    void emptyByteArray(byte[] array) {
        if (array == null) {
            return;
        }
        Arrays.fill(array, (byte) 0);
    }

    void emptyCharArray(char[] array) {
        if (array == null) {
            return;
        }
        Arrays.fill(array, '\0');
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateBundle that = (CertificateBundle) o;
        return secretPropertiesPath.equals(that.secretPropertiesPath) &&
                Arrays.equals(passphrase, that.passphrase) &&
                Arrays.equals(privateKey, that.privateKey) &&
                Arrays.equals(publicCert, that.publicCert) &&
                Arrays.equals(archiveCert, that.archiveCert) &&
                protocol.equals(that.protocol);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(secretPropertiesPath, protocol);
        result = 31 * result + Arrays.hashCode(passphrase);
        result = 31 * result + Arrays.hashCode(privateKey);
        result = 31 * result + Arrays.hashCode(publicCert);
        result = 31 * result + Arrays.hashCode(archiveCert);
        return result;
    }

    @Override
    public String toString() {
        return "CertificateBundle{" +
                "secretPropertiesPath=" + secretPropertiesPath +
                '}';
    }

    static class Builder {
        private Path secretFileNamePath;
        private char[] passphrase;
        private char[] privateKey;
        private char[] publicCert;
        private byte[] archiveCert;

        Builder secretFileNamePath(Path secretFileNamePath) {
            this.secretFileNamePath = secretFileNamePath;
            return this;
        }

        Builder passphrase(char[] passphrase) {
            this.passphrase = passphrase;
            return this;
        }

        Builder privateKey(char[] privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        Builder publicCert(char[] publicCert) {
            this.publicCert = publicCert;
            return this;
        }

        Builder archiveCert(byte[] archiveCert) {
            this.archiveCert = archiveCert;
            return this;
        }

        CertificateBundle build() {
            if (privateKey != null && publicCert != null) {
                return new CertificateBundle(secretFileNamePath, passphrase, privateKey, publicCert);
            } else {
                return new CertificateBundle(secretFileNamePath, passphrase, archiveCert);
            }
        }
    }
}
