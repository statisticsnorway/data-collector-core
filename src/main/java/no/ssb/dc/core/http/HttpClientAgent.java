package no.ssb.dc.core.http;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import no.ssb.dc.api.Execution;
import no.ssb.dc.api.http.Client;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class HttpClientAgent {

    public static void premain(String agentArgs, Instrumentation inst) {
        new AgentBuilder.Default()
                .type(hasSuperType(named(Client.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> builder
                        .method(named("send")).intercept(MethodDelegation.to(HttpClientExporter.Send.class))
                        .method(named("sendAsync")).intercept(MethodDelegation.to(HttpClientExporter.SendAsync.class)))
                .type(hasSuperType(named(Execution.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> {
                    if ("GetHandler".equals(typeDescription.getSimpleName())) {
                        return builder
                                .method(named("execute")).intercept(MethodDelegation.to(HttpClientExporter.GetHandlerInterceptor.class));
                    }
                    return builder;
                })
                .installOn(inst);
    }

}
