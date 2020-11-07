package no.ssb.dc.core.executor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.Termination;
import no.ssb.dc.api.TerminationException;
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
import no.ssb.dc.api.security.ProvidedBusinessSSLResource;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.handler.ParallelHandler;
import no.ssb.dc.core.health.HealthThreadsResource;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Worker {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final UUID workerId;
    private final String specificationId;
    private final String name;
    private final List<WorkerObserver> workerObservers;
    private final Node node;
    private final ExecutionContext context;
    private final boolean keepContentStoreOpenOnWorkerCompletion;

    public Worker(String specificationId, String name, List<WorkerObserver> workerObservers, Node node, ExecutionContext context, boolean keepContentStoreOpenOnWorkerCompletion) {
        this.specificationId = specificationId;
        this.name = name;
        this.workerObservers = workerObservers;
        this.workerId = ULIDGenerator.toUUID(ULIDGenerator.generate());
        this.node = node;
        this.context = context;
        this.keepContentStoreOpenOnWorkerCompletion = keepContentStoreOpenOnWorkerCompletion;
    }

    public static WorkerBuilder newBuilder() {
        return new WorkerBuilder();
    }

    public String getSpecificationId() {
        return specificationId;
    }

    public String getSpecificationName() {
        return name;
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
        AtomicBoolean finishWorkerObserverIsFired = new AtomicBoolean(false);
        AtomicBoolean threadPoolIsTerminated = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            if (!workerObservers.isEmpty()) {
                if (LOG.isDebugEnabled()) LOG.debug("[{}] Fire observer onStart", workerId);
                WorkerObservable workerObservable = new WorkerObservable(workerId, specificationId, context);
                for (WorkerObserver workerObserver : workerObservers) {
                    workerObserver.start(workerObservable);
                }
                startWorkerObserverIsFired.set(true);
            }

            if (LOG.isDebugEnabled()) LOG.debug("[{}] Initialize worker monitor", workerId);
            initializeMonitor();

            if (LOG.isDebugEnabled()) LOG.debug("[{}] Before execute worker node", workerId);

            // =============================================================================================
            // Execute start node
            //
            ExecutionContext output = Executor.execute(node, context);
            //
            // =============================================================================================

            if (LOG.isDebugEnabled()) LOG.debug("[{}] After execute worker node", workerId);

            if (!keepContentStoreOpenOnWorkerCompletion) {
                if (LOG.isDebugEnabled()) LOG.debug("[{}] Normal close and remove topic", workerId);
                ContentStore contentStore = context.services().get(ContentStore.class);
                contentStore.closeTopic(node.configurations().flowContext().topic());
            }

            if (LOG.isDebugEnabled()) LOG.debug("[{}] Normal shutdown of thread-pool", workerId);
            FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
            threadPool.shutdownAndAwaitTermination();
            threadPoolIsTerminated.set(true);

            return output;

        } catch (RuntimeException | Error e) {
            if (LOG.isDebugEnabled()) LOG.debug("[{}] Worker RuntimeException or Error occurred!", workerId);
            failure.set(e);
            throw e;
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) LOG.debug("[{}] Worker Exception occurred!", workerId);
            failure.set(e);
            throw new WorkerException(e);
        } finally {
            WorkerStatus workerStatus = null;
            try {
                if (LOG.isDebugEnabled()) LOG.debug("[{}] Finally update WorkerStatus", workerId);
                if (failure.get() == null) {
                    workerStatus = WorkerStatus.COMPLETED;
                } else if (failure.get() instanceof TerminationException || failure.get().getCause() instanceof TerminationException) {
                    workerStatus = WorkerStatus.CANCELED;
                } else {
                    workerStatus = WorkerStatus.FAILED;
                }

                HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);
                if (monitor != null) {
                    if (LOG.isDebugEnabled()) LOG.debug("[{}] Finally update worker monitor", workerId);
                    monitor.setStatus(workerStatus);

                    monitor.setEndedTimestamp();

                    if (WorkerStatus.FAILED == workerStatus) {
                        try {
                            HealthThreadsResource healthThreadsResource = new HealthThreadsResource();
                            ObjectNode threadDumpNode = JsonParser.createJsonParser().mapper().convertValue(healthThreadsResource.resource(), ObjectNode.class);
                            monitor.setThreadDumpNode(threadDumpNode);
                            monitor.setFailureCause(CommonUtils.captureStackTrace(failure.get()));
                        } catch (Exception e) {
                            LOG.error("Error occurred during serialization of HealthThreadsResource!\n{}", CommonUtils.captureStackTrace(e));
                            if (failure.get() == null) {
                                failure.set(e);
                            }
                        }
                    }
                }

                ContentStore contentStore = context.services().get(ContentStore.class);
                if (!keepContentStoreOpenOnWorkerCompletion && !contentStore.isClosed()) {
                    if (LOG.isDebugEnabled()) LOG.debug("[{}] Finally close content store!", workerId);
                    contentStore.closeTopic(node.configurations().flowContext().topic());
                }

                if (startWorkerObserverIsFired.get() && !workerObservers.isEmpty()) {
                    if (LOG.isDebugEnabled()) LOG.debug("[{}] Finally fire observer onFinish", workerId);
                    WorkerObservable workerObservable = new WorkerObservable(workerId, specificationId, context);
                    List<WorkerObserver> workerObserverList = new ArrayList<>(workerObservers);
                    Collections.reverse(workerObserverList);
                    for (WorkerObserver workerObserver : workerObserverList) {
                        workerObserver.finish(workerObservable, workerStatus);
                    }
                    finishWorkerObserverIsFired.set(true);
                }

                if (!threadPoolIsTerminated.get()) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("[{}] Finally terminate thread-pool if worker exception occurred!", workerId);
                    FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
                    threadPool.shutdownAndAwaitTermination();
                    threadPoolIsTerminated.set(true);
                }

            } catch (Exception e) {
                if (LOG.isDebugEnabled()) LOG.debug("Finally exception occurred!");
                if (failure.get() != null) {
                    if (LOG.isDebugEnabled()) LOG.debug("[{}] Finally exception occurred addSuppressed", workerId);
                    failure.get().addSuppressed(e);
                } else {
                    if (LOG.isDebugEnabled()) LOG.debug("[{}] Finally setFailure exception occurred", workerId);
                    failure.set(e);
                }

                if (startWorkerObserverIsFired.get() && !finishWorkerObserverIsFired.get() && !workerObservers.isEmpty()) {
                    WorkerObservable workerObservable = new WorkerObservable(workerId, specificationId, context);
                    List<WorkerObserver> workerObserverList = new ArrayList<>(workerObservers);
                    Collections.reverse(workerObserverList);
                    for (WorkerObserver workerObserver : workerObserverList) {
                        workerObserver.finish(workerObservable, workerStatus);
                    }
                    finishWorkerObserverIsFired.set(true);
                }
            }

            Throwable throwable = failure.get();
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else if (throwable instanceof Error) {
                throw (Error) throwable;
            } else if (throwable instanceof Exception) {
                throw new WorkerException(throwable);
            }
        }
    }

    private void initializeMonitor() {
        HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);
        if (monitor == null) {
            return;
        }

        monitor.setSpecificationId(specificationId);
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

        ConfigurationMap configurationMap = context.services().get(ConfigurationMap.class);
        if (configurationMap != null) {
            int httpClientTimeoutSeconds = Integer.parseInt(configurationMap.get("data.collector.http.client.timeout.seconds"));
            monitor.request().setHttpClientTimeoutSeconds(httpClientTimeoutSeconds);
            int httpRequestTimeoutSeconds = Integer.parseInt(configurationMap.get("data.collector.http.request.timeout.seconds"));
            monitor.request().setHttpRequestTimeoutSeconds(httpRequestTimeoutSeconds);
        }

        ContentStore contentStore = context.services().get(ContentStore.class);
        if (contentStore != null) {
            String topicName = context.state("global.topic");
            monitor.contentStream().setTopic(topicName);
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

    public void terminate() {
        Termination termination = context.services().get(Termination.class);
        termination.terminate();
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
        private Supplier<ProvidedBusinessSSLResource> businessSSLResourceSupplier;

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
            this.configurationMap = new ConfigurationMap(new LinkedHashMap<>(configurationMap));
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

        public WorkerBuilder useBusinessSSLResourceSupplier(Supplier<ProvidedBusinessSSLResource> businessSSLResourceSupplier) {
            this.businessSSLResourceSupplier = businessSSLResourceSupplier;
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

            services.register(Termination.class, Termination.create());
            services.register(BufferedReordering.class, new BufferedReordering<String>());
            services.register(FixedThreadPool.class, FixedThreadPool.newInstance(
                    configurationMap.contains("data.collector.worker.threads") ? Integer.parseInt(configurationMap.get("data.collector.worker.threads")) : -1)
            );

            String specificationId;
            String name;
            Node targetNode;
            if (specificationBuilder != null) {
                specificationId = specificationBuilder.getId();
                name = specificationBuilder.getName();
                if (printConfiguration) {
                    LOG.info("Execute specification:\n{}", specificationBuilder.serializeAsYaml());
                }
                targetNode = specificationBuilder.end().startFunction();
            } else if (nodeBuilder != null) {
                specificationId = nodeBuilder.getClass().getSimpleName();
                name = nodeBuilder.getClass().getSimpleName();
                if (printConfiguration) {
                    LOG.info("Execute specification:\n{}", nodeBuilder.serializeAsYaml());
                }
                targetNode = nodeBuilder.build();
            } else {
                throw new RuntimeException("Flow- or NodeBuilder is undefined!");
            }

            if (targetNode == null) {
                throw new RuntimeException("Start function is incorrect!");
            }

            if (printExecutionPlan) {
                LOG.info("Execution plan:\n{}", targetNode.toPrintableExecutionPlan());
            }

            if ((sslFactoryScanDirectory != null && sslFactoryBundleName == null) || (businessSSLResourceSupplier != null && sslFactoryBundleName == null)) {
                Security nodeSecurityConfig = targetNode.configurations().security();
                if (nodeSecurityConfig == null) {
                    throw new RuntimeException("Found CertificateFactory, but now bundleName is defined in neither Worker or FlowBuilder");
                }
                sslFactoryBundleName = nodeSecurityConfig.sslBundleName();
            }

            if (!configurationMap.contains("data.collector.http.client.timeout.seconds")) {
                configurationMap.put("data.collector.http.client.timeout.seconds", "20");
            }

            if (!configurationMap.contains("data.collector.http.request.timeout.seconds")) {
                configurationMap.put("data.collector.http.request.timeout.seconds", "15");
            }

            Client.Builder clientBuilder = Client.newClientBuilder();
            if (configurationMap.contains("data.collector.http.version")) {
                Client.Version httpVersion = Client.Version.valueOf(configurationMap.get("data.collector.http.version").toUpperCase());
                clientBuilder.version(httpVersion);
            }
            if (configurationMap.contains("data.collector.http.followRedirects")) {
                Client.Redirect redirectPolicy = Client.Redirect.valueOf(configurationMap.get("data.collector.http.followRedirects").toUpperCase());
                clientBuilder.followRedirects(redirectPolicy);
            }

            CertificateFactory sslFactory = (sslFactoryScanDirectory != null ?
                    CertificateFactory.scanAndCreate(sslFactoryScanDirectory) :
                    businessSSLResourceSupplier == null ?
                            null :
                            // the supplier make requests to Google Secret Manager
                            CertificateFactory.create(businessSSLResourceSupplier.get())
            );
            if (sslFactory != null) {
                services.register(CertificateFactory.class, sslFactory);
            }
            if (sslFactory != null && sslFactoryBundleName != null) {
                CertificateContext certificateContext = sslFactory.getCertificateContext(sslFactoryBundleName);
                clientBuilder.sslContext(certificateContext.sslContext());
                clientBuilder.x509TrustManager(certificateContext.trustManager());
            }

            clientBuilder.connectTimeout(Duration.ofSeconds(Long.parseLong(configurationMap.get("data.collector.http.client.timeout.seconds"))));
            Client client = clientBuilder.build();
            LOG.info("Configured HttpClient version: {}", client.version());
            services.register(Client.class, client);

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
                LOG.warn("ContentStore should be provided to WorkerBuilder! Creating a new instance.");
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

            return new Worker(specificationId, name, workerObservers, targetNode, executionContext, keepContentStoreOpenOnWorkerCompletion);
        }
    }
}
