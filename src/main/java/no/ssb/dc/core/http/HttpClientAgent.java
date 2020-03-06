package no.ssb.dc.core.http;

import net.bytebuddy.agent.builder.AgentBuilder;
import no.ssb.dc.api.http.Client;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class HttpClientAgent {

    public static void main(String agentArgs, Instrumentation inst) throws Exception {
        new AgentBuilder.Default()
                .type(hasSuperType(named(Client.class.getName())))
                .transform(new AgentBuilder.Transformer.ForAdvice()
                        .include(HttpClientAgent.class.getClassLoader())
                        .advice(named("send"), HttpClientAdvice.class.getName())
                )
                .installOn(inst);
    }
}
