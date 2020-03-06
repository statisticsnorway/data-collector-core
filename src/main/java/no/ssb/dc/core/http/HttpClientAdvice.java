package no.ssb.dc.core.http;

import net.bytebuddy.asm.Advice;
import no.ssb.dc.api.http.Request;

public class HttpClientAdvice {

    @Advice.OnMethodEnter
    public static void before(Request request) {
        System.err.println("------------ before");
    }

    @Advice.OnMethodExit
    public static void after(Request request) {
        System.err.println("------------ after");
    }

}
