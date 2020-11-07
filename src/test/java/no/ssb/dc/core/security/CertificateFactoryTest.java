package no.ssb.dc.core.security;

import no.ssb.dc.api.security.BusinessSSLResource;
import no.ssb.dc.api.util.CommonUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static no.ssb.dc.api.security.BusinessSSLResource.safeConvertBytesToCharArrayAsUTF8;
import static org.junit.jupiter.api.Assertions.*;

public class CertificateFactoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(CertificateFactoryTest.class);

    @Test
    public void thatCertificatesAreDiscoveredAndLoaded() {
        Path currentDir = CommonUtils.currentPath();
        CertificateScanner scanner = new CertificateScanner(currentDir);
        scanner.scan();
        scanner.getCertificateBundles().forEach((bundleName, bundle) -> {
            assertNotNull(bundleName);
            assertNotNull(bundle.secretPropertiesPath().orElse(null));
            assertNotNull(bundle.passphrase);
            if (bundle.isArchive()) {
                assertNotNull(bundle.archiveCert);
            } else {
                assertNotNull(bundle.privateKey);
                assertNotNull(bundle.publicCert);
            }
            LOG.trace("bundle: {} -> {}", bundleName, bundle);
        });
        LOG.trace("map: {}", scanner.getCertificateBundles());
    }

    @Disabled
    @Test
    void thatProvidedCertificateIsLoaded() throws IOException {
        Path certsDir = Paths.get("/Volumes/SSB BusinessSSL/certs").resolve("ske-p12-certs");
        Properties props = new Properties();
        props.load(new StringReader(Files.readString(certsDir.resolve("secret.properties"))));

        BusinessSSLResource sslBundle = new BusinessSSLResource() {
            @Override
            public String bundleName() {
                return "ssb-p12-certs";
            }

            @Override
            public String getType() {
                return "p12";
            }

            @Override
            public char[] publicCertificate() {
                return new char[0];
            }

            @Override
            public char[] privateCertificate() {
                return new char[0];
            }

            @Override
            public byte[] archiveCertificate() {
                try {
                    return Files.readAllBytes(certsDir.resolve(props.getProperty("archive.certificate")));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public char[] passphrase() {
                return safeConvertBytesToCharArrayAsUTF8(props.getProperty("secret.passphrase").getBytes());
            }

            @Override
            public void close() {

            }
        };
        CertificateFactory factory = CertificateFactory.create(sslBundle);
        assertFalse(factory.getBundleNames().isEmpty());
    }

    @Disabled
    @Test
    public void thatCertificateFactoryLoadLocalBundles() {
        Path currentDir = CommonUtils.currentPath();
        CertificateFactory factory = CertificateFactory.scanAndCreate(currentDir);
        assertTrue(factory.getBundleNames().contains("ske-test-certs"));
    }

    @Disabled
    @Test
    public void thatCertificateFactoryLoadMountedBundles() {
        Path currentDir = Paths.get("/Volumes/SSB BusinessSSL/certs");
        CertificateFactory factory = CertificateFactory.scanAndCreate(currentDir);
        assertTrue(factory.getBundleNames().contains("ske-p12-certs"));
    }

    @Disabled
    @Test
    public void thatProdCertificateFactoryLoadMountedBundles() {
        Path currentDir = Paths.get("/Volumes/SSB BusinessSSL/certs");
        CertificateFactory factory = CertificateFactory.scanAndCreate(currentDir);
        assertTrue(factory.getBundleNames().contains("ske-prod-certs"));
    }
}
