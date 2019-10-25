package no.ssb.dc.core.executor;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.node.FlowContext;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.NodeWithId;
import no.ssb.dc.api.node.Security;
import no.ssb.dc.api.node.builder.NodeBuilder;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.api.ulid.ULIDStateHolder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.handler.ParallelHandler;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import no.ssb.dc.core.security.CertificateFactory;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Worker {

    private static final ULIDStateHolder ulidStateHolder = new ULIDStateHolder();
    private final UUID workerId;
    private final String name;
    private final List<WorkerObserver> workerObservers;
    private final Node node;
    private final ExecutionContext context;
    private final boolean keepContentStoreOpenOnWorkerCompletion;

    public Worker(String name, List<WorkerObserver> workerObservers, Node node, ExecutionContext context, boolean keepContentStoreOpenOnWorkerCompletion) {
        this.name = name;
        this.workerObservers = workerObservers;
        this.workerId = ULIDGenerator.toUUID(ULIDGenerator.nextMonotonicUlid(ulidStateHolder));
        this.node = node;
        this.context = context;
        this.keepContentStoreOpenOnWorkerCompletion = keepContentStoreOpenOnWorkerCompletion;
    }

    public static WorkerBuilder newBuilder() {
        return new WorkerBuilder();
    }

    public UUID getWorkerId() {
        return workerId;
    }

    public ExecutionContext context() {
        return context;
    }

    /*
     * Aimed at test case scenarios where you want to execute subsequent runs
     */
    public void resetMaxNumberOfIterations() {
        context.state(ParallelHandler.MAX_NUMBER_OF_ITERATIONS, null);
        ParallelHandler.countNumberOfIterations.set(-1);
    }

    public ExecutionContext run() {
        AtomicBoolean startWorkerObserverIsFired = new AtomicBoolean(false);
        AtomicBoolean threadPoolIsTerminated = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        try {
            if (!workerObservers.isEmpty()) {
                WorkerObservable workerObservable = new WorkerObservable(workerId, context);
                for (WorkerObserver workerObserver : workerObservers) {
                    workerObserver.start(workerObservable);
                }
                startWorkerObserverIsFired.set(true);
            }

            initializeMonitor();

            ExecutionContext output = Executor.execute(node, context);
            FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
            threadPool.shutdownAndAwaitTermination();
            threadPoolIsTerminated.set(true);

            if (!keepContentStoreOpenOnWorkerCompletion) {
                ContentStore contentStore = context.services().get(ContentStore.class);
                contentStore.closeTopic(node.configurations().flowContext().topic());
            }

            return output;

        } catch (Exception e) {
            failure.set(e);
            throw new WorkerException(e);
        } finally {
            try {
                try {
                    if (startWorkerObserverIsFired.get() && !workerObservers.isEmpty()) {
                        HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);
                        WorkerStatus outcome = failure.get() == null ? WorkerStatus.COMPLETED : WorkerStatus.FAILED;
                        monitor.setStatus(outcome);
                        monitor.setEndedTimestamp();

                        if (WorkerStatus.FAILED == outcome) {
                            monitor.setFailureCause(CommonUtils.captureStackTrace(failure.get()));
                        }

                        WorkerObservable workerObservable = new WorkerObservable(workerId, context);
                        List<WorkerObserver> workerObserverList = new ArrayList<>(workerObservers);
                        Collections.reverse(workerObserverList);
                        for (WorkerObserver workerObserver : workerObserverList) {
                            workerObserver.finish(workerObservable, outcome);
                        }
                    }

                    if (!threadPoolIsTerminated.get()) {
                        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
                        threadPool.shutdownAndAwaitTermination();
                        threadPoolIsTerminated.set(true);
                    }
                } finally {
                    if (!keepContentStoreOpenOnWorkerCompletion) {
                        ContentStore contentStore = context.services().get(ContentStore.class);
                        if (!contentStore.isClosed()) {
                            contentStore.closeTopic(node.configurations().flowContext().topic());
                        }
                    }
                }
            } catch (Exception e) {
                throw new WorkerException(e);
            }
        }
    }

    private void initializeMonitor() {
        HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);
        if (monitor == null) {
            return;
        }

        monitor.setName(name);
        Class<?> startFunctionInterface = node.getClass().isInterface() ? node.getClass() : node.getClass().getInterfaces()[0];
        monitor.setStartFunction(startFunctionInterface.getName());
        monitor.setStartFunctionId(((NodeWithId) node).id());

        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
        monitor.setThreadPoolInfo(threadPool.asThreadPoolInfo());

        Security security = node.configurations().security();
        if (security != null) {
            monitor.security().setSslBundleName(security.sslBundleName());
        }

        ContentStore contentStore = context.services().get(ContentStore.class);
        if (contentStore != null) {
            String topicName = context.state("global.topic");
            monitor.contentStream().setTopic(topicName);
            String lastPosition = contentStore.lastPosition(topicName);
            monitor.contentStream().setLastPosition(lastPosition);
            monitor.contentStream().setMonitor(contentStore.monitor());
        }

        Headers globalRequestHeaders = context.state(Headers.class);
        if (globalRequestHeaders != null) {
            monitor.request().setHeaders(globalRequestHeaders.asMap());
        } else {
            Headers globalConfigurationContextRequestHeaders = node.configurations().flowContext().globalContext().state(Headers.class);
            if (globalConfigurationContextRequestHeaders != null) {
                monitor.request().setHeaders(globalConfigurationContextRequestHeaders.asMap());
            }
        }

        monitor.setStatus(WorkerStatus.RUNNING);
        monitor.setStartedTimestamp();
    }

    public CompletableFuture<ExecutionContext> runAsync() {
        return CompletableFuture
                .supplyAsync(this::run)
                .exceptionally(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    throw new WorkerException(throwable);
                });
    }

    // TODO implkement a callback observer to stop processing from ouside
    public void terminate() {
        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
        threadPool.shutdownAndAwaitTermination();
    }

    public static class WorkerBuilder {

        private static final Logger LOG = LoggerFactory.getLogger(WorkerBuilder.class);
        boolean printExecutionPlan;
        boolean printConfiguration;
        private SpecificationBuilder specificationBuilder;
        private NodeBuilder nodeBuilder;
        private ConfigurationMap configurationMap;
        private Headers headers = new Headers();
        private Services services = Services.create();
        private Map<Object, Object> globalState = new LinkedHashMap<>();
        private Map<String, Object> variables = new LinkedHashMap<>();
        private Map<Object, Object> state = new LinkedHashMap<>();
        private Path sslFactoryScanDirectory;
        private String sslFactoryBundleName;
        private String topicName;
        private List<WorkerObserver> workerObservers = new ArrayList<>();
        private ContentStore contentStore;
        private boolean keepContentStoreOpenOnWorkerCompletion;

        public WorkerBuilder specification(SpecificationBuilder specificationBuilder) {
            this.specificationBuilder = specificationBuilder;
            return this;
        }

        public SpecificationBuilder getSpecificationBuilder() {
            return specificationBuilder;
        }

        public WorkerBuilder specification(NodeBuilder nodeBuilder) {
            this.nodeBuilder = nodeBuilder;
            return this;
        }

        public WorkerBuilder configuration(Map<String, String> configurationMap) {
            this.configurationMap = new ConfigurationMap(configurationMap);
            return this;
        }

        public WorkerBuilder workerObserver(WorkerObserver observer) {
            this.workerObservers.add(observer);
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

        public WorkerBuilder contentStore(ContentStore contentStore) {
            this.contentStore = contentStore;
            return this;
        }

        /**
         * Use only when this is intended. If conent store is not closed after each worker is completet, this may lead to memory leaks.
         *
         * @param keepContentStoreOpenOnWorkerCompletion
         * @return
         */
        public WorkerBuilder keepContentStoreOpenOnWorkerCompletion(boolean keepContentStoreOpenOnWorkerCompletion) {
            this.keepContentStoreOpenOnWorkerCompletion = keepContentStoreOpenOnWorkerCompletion;
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

            String name;
            Node targetNode;
            if (specificationBuilder != null) {
                name = specificationBuilder.getName();
                if (printConfiguration) {
                    LOG.info("Execute specification:\n{}", specificationBuilder.serializeAsYaml());
                }
                targetNode = specificationBuilder.end().startFunction();
            } else if (nodeBuilder != null) {
                name = nodeBuilder.getClass().getSimpleName();
                if (printConfiguration) {
                    LOG.info("Execute specification:\n{}", nodeBuilder.serializeAsYaml());
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
            }
            globalState.put("global.topic", topicName);

            if (!configurationMap.contains("content.stream.connector")) {
                configurationMap.put("content.stream.connector", "discarding");
            }

            if (contentStore == null) {
                contentStore = ProviderConfigurator.configure(configurationMap.asMap(), configurationMap.get("content.stream.connector"), ContentStoreInitializer.class);
            }
            services.register(ContentStore.class, contentStore);

            if (!headers.asMap().isEmpty()) {
                globalState(Headers.class, headers);
            }

            ExecutionContext executionContext = new ExecutionContext.Builder()
                    .services(services)
                    .variables(variables)
                    .globalState(globalState)
                    .state(state)
                    .build();

            return new Worker(name, workerObservers, targetNode, executionContext, keepContentStoreOpenOnWorkerCompletion);
        }
    }
}
