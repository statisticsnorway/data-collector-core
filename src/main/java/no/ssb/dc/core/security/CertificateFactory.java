package no.ssb.dc.core.security;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CertificateFactory {

    private final Map<String, SSLContext> sslContextMap;

    private CertificateFactory(Map<String, SSLContext> sslContextMap) {
        this.sslContextMap = sslContextMap;
    }

    public Set<String> getBundleNames() {
        return sslContextMap.keySet();
    }

    public SSLContext getSSLContext(String bundleName) {
        return sslContextMap.get(bundleName);
    }

    static class Builder {
        Map<String, CertificateBundle> bundles = new LinkedHashMap<>();

        Builder bundle(String bundleName, CertificateBundle bundle) {
            bundles.put(bundleName, bundle);
            return this;
        }

        CertificateFactory build() {
            Map<String, SSLContext> sslContextMap = new LinkedHashMap<>();
            for (Map.Entry<String, CertificateBundle> entry : bundles.entrySet()) {
                SslKeyStore sslKeyStore = new SslKeyStore(entry.getValue());
                String bundleName = entry.getKey();
                SSLContext businessSSLContext = sslKeyStore.buildSSLContext();
                sslContextMap.put(bundleName, businessSSLContext);
            }
            return new CertificateFactory(sslContextMap);
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
