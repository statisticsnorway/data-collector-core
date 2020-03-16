package no.ssb.dc.core.content;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import no.ssb.dc.api.content.ContentStore;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class ContentStoreAgent {

    public static void install(Instrumentation inst) {
        new AgentBuilder.Default()
                .type(hasSuperType(named(ContentStore.class.getName())))
                .transform((builder, typeDescription, classLoader, javaModule) -> builder
                        .method(named("addPaginationDocument")).intercept(MethodDelegation.to(ContentStoreExporter.Paginate.class))
                        .method(named("bufferPaginationEntryDocument")).intercept(MethodDelegation.to(ContentStoreExporter.Entry.class))
                        .method(named("bufferDocument")).intercept(MethodDelegation.to(ContentStoreExporter.Document.class))
                        .method(named("publish")).intercept(MethodDelegation.to(ContentStoreExporter.Publish.class))
                )
                .installOn(inst);
    }
}
