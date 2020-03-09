package no.ssb.dc.core.http;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import no.ssb.dc.api.Execution;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.Callable;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class HttpClientAgent {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientAgent.class);

    public static void premain(String agentArgs, Instrumentation inst) {
        new AgentBuilder.Default()
                .type(hasSuperType(named(Client.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> {
                    System.err.println("0----: " + javaModule.getActualName());
                    return builder
                            .method(named("send")).intercept(MethodDelegation.to(HttpClientExporter.Send.class))
                            .method(named("sendAsync")).intercept(MethodDelegation.to(HttpClientExporter.SendAsync.class));

                })
                .type(hasSuperType(named(Execution.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> {
                    System.err.println("1----: " + javaModule.getActualName());
                    return builder
                            .method(named("execute")).intercept(MethodDelegation.to(GetHandlerInterceptor.class));
                })
                .installOn(inst);
    }

    static class GetHandlerInterceptor {

        static ExecutionContext intercept(@SuperCall Callable<ExecutionContext> zuper, @Argument(0) ExecutionContext context) throws Exception {
            System.err.println("==========================================================================================");
            return null;
        }
    }
}
