package no.ssb.dc.core.security;

import no.ssb.dc.api.security.ProvidedBusinessSSLResource;
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
                SslKeyStore sslKeyStore = entry.getValue().isArchive() ? new SslP12KeyStore(entry.getValue()) : new SslPEMKeyStore(entry.getValue());
                String bundleName = entry.getKey();
                CertificateContext businessSSLContext = sslKeyStore.buildSSLContext();
                LOG.info("Loaded certificate {} bundle: {}", (entry.getValue().isArchive() ? "P12" : "PEM"), bundleName);
                certificateContextMap.put(bundleName, businessSSLContext);
            }
            return new CertificateFactory(certificateContextMap);
        }
    }

    public static CertificateFactory scanAndCreate(Path scanDirectory) {
        LOG.info("Certificate location: {}", scanDirectory.toAbsolutePath().normalize().toString());
        CertificateScanner scanner = new CertificateScanner(scanDirectory);
        scanner.scan();
        CertificateFactory.Builder builder = new CertificateFactory.Builder();
        scanner.getCertificateBundles().forEach(builder::bundle);
        return builder.build();
    }

    public static CertificateFactory create(ProvidedBusinessSSLResource providedBusinessSSLResource) {
        Objects.requireNonNull(providedBusinessSSLResource);
        LOG.info("Create Certificate: {}", providedBusinessSSLResource.bundleName());
        CertificateFactory.Builder factoryBuilder = new CertificateFactory.Builder();

        CertificateBundle.Builder bundleBuilder = new CertificateBundle.Builder();
        if (providedBusinessSSLResource.isPEM()) {
            bundleBuilder.publicCert(providedBusinessSSLResource.publicCertificate());
            bundleBuilder.privateKey(providedBusinessSSLResource.privateCertificate());
        } else {
            bundleBuilder.archiveCert(providedBusinessSSLResource.archiveCertificate());
        }
        bundleBuilder.passphrase(providedBusinessSSLResource.passphrase());
        bundleBuilder.build();

        factoryBuilder.bundle(providedBusinessSSLResource.bundleName(), bundleBuilder.build());
        return factoryBuilder.build();
    }

}
