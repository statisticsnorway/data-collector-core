package no.ssb.dc.core.security;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CertificateFactory {

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
                SslKeyStore sslKeyStore = new SslKeyStore(entry.getValue());
                String bundleName = entry.getKey();
                CertificateContext businessSSLContext = sslKeyStore.buildSSLContext();
                certificateContextMap.put(bundleName, businessSSLContext);
            }
            return new CertificateFactory(certificateContextMap);
        }
    }

    public static CertificateFactory scanAndCreate(Path scanDirectory) {
        CertificateScanner scanner = new CertificateScanner(scanDirectory);
        scanner.scan();
        CertificateFactory.Builder builder = new CertificateFactory.Builder();
        scanner.getCertificateBundles().forEach(builder::bundle);
        return builder.build();
    }

}
