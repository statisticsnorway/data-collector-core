package no.ssb.dc.core.security;

import no.ssb.dc.api.util.CommonUtils;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CertificateFactory {

    private Map<String, SSLContext> sslContextMap;

    private CertificateFactory(Map<String, SSLContext> sslContextMap) {
        this.sslContextMap = sslContextMap;
    }

    public Set<String> getBundleNames() {
        return sslContextMap.keySet();
    }

    public SSLContext getSSLContext(String bundleName) {
        return sslContextMap.get(bundleName);
    }

    public static class Builder {
        Map<String, CertificateBundle> bundles = new LinkedHashMap<>();

        public Builder() {
        }

        public Builder bundle(String bundleName, CertificateBundle bundle) {
            bundles.put(bundleName, bundle);
            return this;
        }

        public CertificateFactory build() {
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

    public static CertificateFactory create(Path scanDirectory) {
        CertificateScanner scanner = new CertificateScanner(CommonUtils.currentPath());
        scanner.scan();
        CertificateFactory.Builder builder = new CertificateFactory.Builder();
        scanner.getCertificateBundles().forEach(builder::bundle);
        return builder.build();
    }

}
