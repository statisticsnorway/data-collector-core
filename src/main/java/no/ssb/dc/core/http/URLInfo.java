package no.ssb.dc.core.http;

import java.net.MalformedURLException;
import java.net.URL;

public class URLInfo {
    private final URL url;

    public URLInfo(String url) {
        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getLocation() {
        return String.format("%s://%s%s", url.getProtocol(), url.getHost(), (-1 == url.getPort() || 80 == url.getPort() || 443 == url.getPort() ? "" : ":" + url.getPort()));
    }

    public String getRequestPath() {
        return url.getPath();
    }
}
