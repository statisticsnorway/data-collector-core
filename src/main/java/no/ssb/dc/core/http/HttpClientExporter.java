package no.ssb.dc.core.http;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public class HttpClientExporter {

    static class Send {
        static Response intercept(@SuperCall Callable<Response> zuper, @Argument(0) Request request) throws Exception {
            System.out.println("Client.send(): " + request);
            Response response = zuper.call();
            System.out.println("Client.send() response: " + response.url());
            return response;
        }
    }

    static class SendAsync {
        static CompletableFuture<Response> intercept(@SuperCall Callable<CompletableFuture<Response>> zuper, @Argument(0) Request request) throws Exception {
            System.out.println("Client.sendAsync(): " + request);
            CompletableFuture<Response> future = zuper.call();
            future.thenApply(response -> {
                System.out.println("Client.sendAsync() response: " + response.url());
                return response;
            });
            return future;
        }
    }

}
