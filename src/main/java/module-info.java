import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;

module no.ssb.dc.core {
    requires jdk.unsupported;
    requires java.base;
    requires java.logging;
    requires java.net.http;
    requires hystrix.core;
    requires org.slf4j;
    requires jul_to_slf4j;
    requires commons.jexl3;
    requires java.xml;
    requires io.github.classgraph;
    requires net.bytebuddy;
    requires net.bytebuddy.agent;
    requires org.reactivestreams;
    requires io.reactivex.rxjava3;
    requires de.huxhorn.sulky.ulid;

    requires no.ssb.config;
    requires no.ssb.service.provider.api;
    requires no.ssb.rawdata.api;
    requires no.ssb.dc.api;

    provides Client.Builder with no.ssb.dc.core.http.HttpClientDelegate.ClientBuilder;
    provides Request.Builder with no.ssb.dc.core.http.HttpRequestDelegate.RequestBuilder;
    provides Response.Builder with no.ssb.dc.core.http.HttpResponseDelegate.ResponseBuilder;

    exports no.ssb.dc.core.executor;
    exports no.ssb.dc.core.handler;
    exports no.ssb.dc.core.http;
    exports no.ssb.dc.core.util;
}
