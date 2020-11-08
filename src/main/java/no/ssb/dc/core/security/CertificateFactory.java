package no.ssb.dc.core.security;

import no.ssb.dc.api.security.BusinessSSLResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CertificateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CertificateFactory.class);

    private final Map<String, CertificateContext> certificateContextMap;

    private CertificateFactory(Map<String, CertificateContext> certificateContextMap) {
        this.certificateContextMap = certificateContextMap;
    }

    public Set<String> getBundleNames() {
        return certificateContextMap.keySet();
    }

    public CertificateContext getCertificateContext(String bundleName) {
        if (!certificateContextMap.containsKey(bundleName)) {
            throw new RuntimeException("Unable to resolve certificate bundle: " + bundleName);
        }
        return certificateContextMap.get(bundleName);
    }

    static class Builder {
        Map<String, CertificateBundle> bundles = new LinkedHashMap<>();

        Builder bundle(String bundleName, CertificateBundle bundle) {
            bundles.put(bundleName, bundle);
            return this;
        }

        CertificateFactory build() {
            Map<String, CertificateContext> certificateContextMap = new LinkedHashMap<>();
            for (Map.Entry<String, CertificateBundle> entry : bundles.entrySet()) {
                CertificateBundle certificateBundle = entry.getValue();
                try {
                    SslKeyStore sslKeyStore = certificateBundle.isArchive() ? new SslP12KeyStore(certificateBundle) : new SslPEMKeyStore(certificateBundle);
                    String bundleName = entry.getKey();
                    CertificateContext businessSSLContext = sslKeyStore.buildSSLContext();
                    LOG.info("Loaded certificate {} bundle: {}", (certificateBundle.isArchive() ? "P12" : "PEM"), bundleName);
                    certificateContextMap.put(bundleName, businessSSLContext);
                } finally {
                    certificateBundle.dispose(); // dispose secrets from heap
                }
            }
            return new CertificateFactory(certificateContextMap);
        }
    }

    public static CertificateFactory scanAndCreate(Path scanDirectory) {
        LOG.info("Certificate location: {}", scanDirectory.toAbsolutePath().normalize().toString());
        CertificateScanner scanner = new CertificateScanner(scanDirectory); // TODO refactor CertificateScanner.findSecurityPropertyFilesAndLoad to use BusinessSSLResource
        scanner.scan();
        CertificateFactory.Builder factoryBuilder = new CertificateFactory.Builder();
        scanner.getCertificateBundles().forEach(factoryBuilder::bundle);
        return factoryBuilder.build();
    }

    public static CertificateFactory create(BusinessSSLResource businessSSLResource) {
        Objects.requireNonNull(businessSSLResource);
        // load secrets
        try (businessSSLResource) {
            LOG.info("Create Certificate: {}", businessSSLResource.bundleName());
            CertificateFactory.Builder factoryBuilder = new CertificateFactory.Builder();

            CertificateBundle.Builder bundleBuilder = new CertificateBundle.Builder();
            if (businessSSLResource.isPEM()) {
                bundleBuilder.publicCert(businessSSLResource.publicCertificate());
                bundleBuilder.privateKey(businessSSLResource.privateCertificate());
            } else {
                bundleBuilder.archiveCert(businessSSLResource.archiveCertificate());
            }
            bundleBuilder.passphrase(businessSSLResource.passphrase());
            bundleBuilder.build();

            factoryBuilder.bundle(businessSSLResource.bundleName(), bundleBuilder.build());
            return factoryBuilder.build();
        }
    }

}
