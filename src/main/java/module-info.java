import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;

module no.ssb.dc.core {
    requires no.ssb.config;
    requires no.ssb.service.provider.api;
    requires no.ssb.rawdata.api;
    requires no.ssb.dc.api;
    requires dapla.secrets.client.api;
    requires dapla.secrets.provider.safe.configuration;
    requires dapla.secrets.provider.dynamic.configuration;

    requires java.logging;
    requires java.instrument;
    requires java.net.http;

    requires methanol;
    requires okhttp3;

    requires org.slf4j;
    requires jul_to_slf4j;

    requires net.bytebuddy;
    requires net.bytebuddy.agent;

    requires org.reactivestreams;
    requires io.reactivex.rxjava3;
    requires de.huxhorn.sulky.ulid;
    requires commons.jexl3;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires java.xml;
    requires hystrix.core;
    requires io.github.classgraph;
    requires org.bouncycastle.pkix;
    requires org.bouncycastle.provider;
    requires jackson.jq;

    requires java.jwt;

    requires simpleclient.common;
    requires simpleclient;
    requires simpleclient.hotspot;

    provides Client.Builder with no.ssb.dc.core.http.HttpClientDelegate.ClientBuilder;
    provides Request.Builder with no.ssb.dc.core.http.HttpRequestDelegate.RequestBuilder;
    provides Response.Builder with no.ssb.dc.core.http.HttpResponseDelegate.ResponseBuilder;

    opens no.ssb.dc.core;
    opens no.ssb.dc.core.server;
    opens no.ssb.dc.core.handler;

    exports no.ssb.dc.core.content;
    exports no.ssb.dc.core.executor;
    exports no.ssb.dc.core.handler;
    exports no.ssb.dc.core.http;
    exports no.ssb.dc.core.health;
    exports no.ssb.dc.core.metrics;
    exports no.ssb.dc.core.security;
    exports no.ssb.dc.core.util;

    // TODO API requires access to Core test scope. Added CircumventIllegalModulePackage to allow package exports and opens.
    exports no.ssb.dc.core.service;
    exports no.ssb.dc.core.controller;
}
