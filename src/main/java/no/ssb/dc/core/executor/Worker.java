package no.ssb.dc.core.executor;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Paginate;

public class Worker {

    final Paginate paginate;
    final ExecutionContext input;
    final Lifecycle lifecycle;

    public Worker(Paginate paginate, ExecutionContext input) {
        this.paginate = paginate;
        this.input = input;

        DynamicConfiguration configuration = input.services().get(DynamicConfiguration.class);
        FixedThreadPool fixedThreadPool = input.services().get(FixedThreadPool.class);
//        RawdataClient rawdataClient = input.services().get(RawdataClient.class);
//        RawdataProducer producer = rawdataClient.producer(configuration.evaluateToString("namespace.default"));

        this.lifecycle = new Lifecycle(fixedThreadPool, null);
    }

    public ExecutionContext run() throws InterruptedException {
        ExecutionContext dummyOutput = ExecutionContext.of(input);

        /*
         * non-blocking call and triggers PaginateHandler.execute
         * PaginateHandler.execute triggers executution of sequence and nextPage, which eventually fires LifecycleCallBacks
         */
        lifecycle.execute(paginate, input);

//        AtomicBoolean endOfStream = new AtomicBoolean(false);
//        while (!endOfStream.get()) {
//            ItemListContext task = lifecycle.getListItemTasks().poll(1, TimeUnit.SECONDS);
//
//            if (task == null) {
//                endOfStream.set(true);
//                continue;
//            }
//            System.out.printf("task: %s%n", task.beginInclusive());
//        }

        return dummyOutput;
    }

}
