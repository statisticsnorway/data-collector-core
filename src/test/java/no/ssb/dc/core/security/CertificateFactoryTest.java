package no.ssb.dc.core.security;

import no.ssb.dc.api.util.CommonUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CertificateFactoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(CertificateFactoryTest.class);

    @Test
    public void thatCertificatesAreDiscoveredAndLoaded() {
        Path currentDir = CommonUtils.currentPath();
        CertificateScanner scanner = new CertificateScanner(currentDir);
        scanner.scan();
        scanner.getCertificateBundles().forEach((bundleName, bundle) -> {
            assertNotNull(bundleName);
            assertNotNull(bundle.secretPropertiesPath);
            assertNotNull(bundle.passphrase);
            assertNotNull(bundle.privateKey);
            assertNotNull(bundle.publicCert);
            LOG.trace("bundle: {} -> {}", bundleName, bundle);
        });
        LOG.trace("map: {}", scanner.getCertificateBundles());
    }

    @Disabled
    @Test
    public void thatCertificateFactoryLoadBundles() {
        Path currentDir = CommonUtils.currentPath();
        CertificateFactory factory = CertificateFactory.scanAndCreate(currentDir);
        assertTrue(factory.getBundleNames().contains("ske-test-certs"));
    }

    @Disabled
    @Test
    public void thatProdCertificateFactoryLoadBundles() {
        Path currentDir = Paths.get("/Volumes/SSB BusinessSSL/certs");
        CertificateFactory factory = CertificateFactory.scanAndCreate(currentDir);
        assertTrue(factory.getBundleNames().contains("ske-prod-certs"));
    }
}
