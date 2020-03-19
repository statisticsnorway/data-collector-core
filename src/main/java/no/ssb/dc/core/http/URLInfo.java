package no.ssb.dc.core.http;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

public class URLInfo {
    private final URL url;

    public URLInfo(String url) {
        try {
            this.url = (url != null ? new URL(url) : null);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getLocation() {
        if (url == null) {
            return Optional.empty();
        }
        return Optional.of(String.format("%s://%s%s", url.getProtocol(), url.getHost(), (-1 == url.getPort() || 80 == url.getPort() || 443 == url.getPort() ? "" : ":" + url.getPort())));
    }

    public Optional<String> getRequestPath() {
        if (url == null) {
            return Optional.empty();
        }
        return Optional.of(url.getPath());
    }
}
