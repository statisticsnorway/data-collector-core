package no.ssb.dc.core.http;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import no.ssb.dc.api.http.Client;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperClass;
import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class HttpClientAgent {

    /*
     * Finding with ByteBuddy 1.10.8
     * - The hasSuperClass breaks if you invoke GetHandler.class.getName(). A fully qualified name must be assigned as string!
     * - The hasSuperType accepts interface Object.class.getName()!
     */
    public static void install(Instrumentation inst) {
        new AgentBuilder.Default()
                .type(hasSuperType(named(Client.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> builder
                        .method(named("send")).intercept(MethodDelegation.to(HttpClientExporter.Send.class))
                        .method(named("sendAsync")).intercept(MethodDelegation.to(HttpClientExporter.SendAsync.class)))
                .type(hasSuperClass(named("no.ssb.dc.core.handler.GetHandler")))
                .transform((builder, typeDescription, classLoader, javaModule) -> builder
                        .method(named("execute")).intercept(MethodDelegation.to(HttpClientExporter.GetHandlerInterceptor.class)))
                .installOn(inst);
    }
}
