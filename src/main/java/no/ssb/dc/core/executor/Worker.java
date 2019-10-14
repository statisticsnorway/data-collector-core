package no.ssb.dc.core.executor;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.node.FlowContext;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Security;
import no.ssb.dc.api.node.builder.NodeBuilder;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.core.handler.ParallelHandler;
import no.ssb.dc.core.security.CertificateFactory;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Worker {

    private final Node node;
    private final ExecutionContext context;

    public Worker(Node node, ExecutionContext context) {
        this.node = node;
        this.context = context;
    }

    public static WorkerBuilder newBuilder() {
        return new WorkerBuilder();
    }

    public ExecutionContext run() {
        try {
            ExecutionContext output = Executor.execute(node, context);
            ContentStore contentStore = context.services().get(ContentStore.class);
            contentStore.close();
            return output;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<ExecutionContext> runAsync() {
        return CompletableFuture.supplyAsync(() -> Executor.execute(node, context));
    }

    public static class WorkerBuilder {

        private static final Logger LOG = LoggerFactory.getLogger(WorkerBuilder.class);

        SpecificationBuilder specificationBuilder;
        NodeBuilder nodeBuilder;
        ConfigurationMap configurationMap;
        Headers headers = new Headers();
        Services services = Services.create();
        Map<Object, Object> globalState = new LinkedHashMap<>();
        Map<String, Object> variables = new LinkedHashMap<>();
        Map<Object, Object> state = new LinkedHashMap<>();
        Path sslFactoryScanDirectory;
        String sslFactoryBundleName;
        String topicName;
        String initialPosition;
        String initialPositionVariableName;
        boolean printExecutionPlan;
        boolean printConfiguration;

        public WorkerBuilder specification(SpecificationBuilder specificationBuilder) {
            this.specificationBuilder = specificationBuilder;
            return this;
        }

        public WorkerBuilder specification(NodeBuilder nodeBuilder) {
            this.nodeBuilder = nodeBuilder;
            return this;
        }

        public WorkerBuilder configuration(Map<String, String> configurationMap) {
            this.configurationMap = new ConfigurationMap(configurationMap);
            return this;
        }

        public WorkerBuilder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public WorkerBuilder variable(String name, Object value) {
            variables.put(name, value);
            return this;
        }

        public WorkerBuilder globalState(Object name, Object value) {
            globalState.put(name, value);
            return this;
        }

        public WorkerBuilder state(Object name, Object value) {
            state.put(name, value);
            return this;
        }

        public WorkerBuilder buildCertificateFactory(Path scanDirectory) {
            this.sslFactoryScanDirectory = scanDirectory;
            return this;
        }

        public WorkerBuilder buildCertificateFactory(Path scanDirectory, String bundleName) {
            this.sslFactoryScanDirectory = scanDirectory;
            this.sslFactoryBundleName = bundleName;
            return this;
        }

        public WorkerBuilder topic(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public WorkerBuilder initialPosition(String position) {
            this.initialPosition = position;
            return this;
        }

        public WorkerBuilder initialPositionVariable(String variableName) {
            this.initialPositionVariableName = variableName;
            return this;
        }

        public WorkerBuilder stopAtNumberOfIterations(Integer numberOfIterations) {
            state.put(ParallelHandler.MAX_NUMBER_OF_ITERATIONS, numberOfIterations);
            return this;
        }

        public WorkerBuilder printExecutionPlan() {
            this.printExecutionPlan = true;
            return this;
        }

        public WorkerBuilder printConfiguration() {
            this.printConfiguration = true;
            return this;
        }


        public Worker build() {
            if (configurationMap == null) {
                configurationMap = new ConfigurationMap(new LinkedHashMap<>());
            }
            services.register(ConfigurationMap.class, configurationMap);

            services.register(BufferedReordering.class, new BufferedReordering<String>());
            services.register(FixedThreadPool.class, FixedThreadPool.newInstance(
                    configurationMap.contains("data.collector.worker.threads") ? Integer.parseInt(configurationMap.get("data.collector.worker.threads")) : -1)
            );

            Node targetNode;
            if (specificationBuilder != null) {
                if (printConfiguration) {
                    LOG.info("Serialized flow:\n{}", specificationBuilder.serialize());
                }
                targetNode = specificationBuilder.end().startFunction();
            } else if (nodeBuilder != null) {
                if (printConfiguration) {
                    LOG.info("Serialized flow:\n{}", nodeBuilder.serialize());
                }
                targetNode = nodeBuilder.build();
            } else {
                throw new RuntimeException("Flow- or NodeBuilder is undefined!");
            }
            if (printExecutionPlan) {
                LOG.info("Execution plan:\n{}", targetNode.toPrintableExecutionPlan());
            }

            if (sslFactoryScanDirectory != null && sslFactoryBundleName == null) {
                Security nodeSecurityConfig = targetNode.configurations().security();
                if (nodeSecurityConfig == null) {
                    throw new RuntimeException("Found CertificateFactory, but now bundleName is defined in neither Worker or FlowBuilder");
                }
                sslFactoryBundleName = nodeSecurityConfig.sslBundleName();
            }

            Client.Builder builder = Client.newClientBuilder();
            CertificateFactory sslFactory = (sslFactoryScanDirectory != null && sslFactoryBundleName != null ?
                    CertificateFactory.scanAndCreate(sslFactoryScanDirectory) :
                    null
            );
            if (sslFactory != null) {
                builder.sslContext(sslFactory.getSSLContext(sslFactoryBundleName));
            }
            services.register(Client.class, builder.build());


            if (topicName == null) {
                FlowContext flowContext = targetNode.configurations().flowContext();
                if (flowContext.topic() == null) {
                    throw new RuntimeException("Topic name is undefined in neither Worker, Flow- or NodeBuilder!");
                } else {
                    topicName = flowContext.topic();
                }
            } else {
                globalState.put("global.topic", topicName);
            }

            if (!configurationMap.contains("content.store.provider")) {
                configurationMap.put("content.store.provider", "discarding");
            }

            ContentStore contentStore = ProviderConfigurator.configure(configurationMap.asMap(), configurationMap.get("content.store.provider"), ContentStoreInitializer.class);
            services.register(ContentStore.class, contentStore);


            // set initial position
            if (initialPositionVariableName != null) {
                if (contentStore.lastPosition(topicName) == null) {
                    variable(initialPositionVariableName, initialPosition);
                } else {
                    variable(initialPositionVariableName, contentStore.lastPosition(topicName));
                }
            }

            if (!headers.asMap().isEmpty()) {
                globalState(Headers.class, headers);
            }

            ExecutionContext executionContext = new ExecutionContext.Builder()
                    .services(services)
                    .variables(variables)
                    .globalState(globalState)
                    .state(state)
                    .build();

            return new Worker(targetNode, executionContext);
        }
    }
}
