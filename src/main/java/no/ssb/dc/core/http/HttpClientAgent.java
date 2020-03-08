package no.ssb.dc.core.http;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import no.ssb.dc.api.http.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class HttpClientAgent {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientAgent.class);

    public static void premain(String agentArgs, Instrumentation inst) {
        new AgentBuilder.Default()
                .type(hasSuperType(named(Client.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> builder
                        .method(named("send")).intercept(MethodDelegation.to(HttpClientExporter.Send.class))
                        .method(named("sendAsync")).intercept(MethodDelegation.to(HttpClientExporter.SendAsync.class)))
                .installOn(inst);
    }
}
