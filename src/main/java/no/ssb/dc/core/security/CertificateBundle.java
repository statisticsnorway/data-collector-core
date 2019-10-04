package no.ssb.dc.core.security;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

class CertificateBundle {
    final Path secretPropertiesPath;
    final char[] passphrase;
    final char[] privateKey;
    final char[] publicCert;
    final String protocol = "TLSv1.2";

    private CertificateBundle(Path secretPropertiesPath, char[] passphrase, char[] privateKey, char[] publicCert) {
        this.secretPropertiesPath = secretPropertiesPath;
        this.passphrase = passphrase;
        this.privateKey = privateKey;
        this.publicCert = publicCert;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateBundle bundle = (CertificateBundle) o;
        return secretPropertiesPath.equals(bundle.secretPropertiesPath) &&
                Arrays.equals(passphrase, bundle.passphrase) &&
                Arrays.equals(privateKey, bundle.privateKey) &&
                Arrays.equals(publicCert, bundle.publicCert);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(secretPropertiesPath);
        result = 31 * result + Arrays.hashCode(passphrase);
        result = 31 * result + Arrays.hashCode(privateKey);
        result = 31 * result + Arrays.hashCode(publicCert);
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

        CertificateBundle build() {
            return new CertificateBundle(secretFileNamePath, passphrase, privateKey, publicCert);
        }
    }
}
