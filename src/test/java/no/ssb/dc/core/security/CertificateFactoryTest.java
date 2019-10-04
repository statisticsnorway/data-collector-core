package no.ssb.dc.core.security;

import no.ssb.dc.api.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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

    @Ignore
    @Test
    public void thatCertificateFactoryLoadBundles() {
        Path currentDir = CommonUtils.currentPath();
        CertificateFactory factory = CertificateFactory.scanAndCreate(currentDir);
        assertEquals(factory.getBundleNames(), Set.of("ske-test-certs"));
    }
}
