package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.JobControlListener;
import it.polimi.affetti.tspoon.runtime.StringClient;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryID;
import it.polimi.affetti.tspoon.tgraph.query.QuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.RandomQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.state.RandomSPUSupplier;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdateID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 26/07/17.
 * <p>
 * Sends the ping to keep a rate at source.
 */
public abstract class TunableSource<T extends UniquelyRepresentableForTracking>
        extends RichParallelSourceFunction<T>
        implements JobControlListener {
    protected transient Logger LOG;

    private boolean busyWait = false;
    protected final int baseRate, resolution, batchSize;
    protected int count, numberOfRecordsPerTask;
    protected double resolutionPerTask, currentRate;
    protected long waitPeriodMicro;
    private final String trackingServerNameForDiscovery;

    protected int taskNumber;
    protected volatile boolean stop;

    private BlockingQueue<Optional<T>> elements;

    private Semaphore newBatchSemaphore;

    private transient JobControlClient jobControlClient;
    private transient StringClient requestTrackerClient;
    private transient Thread trackerThread;

    public TunableSource(int baseRate, int resolution, int batchSize, String trackingServerNameForDiscovery) {
        this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
        this.count = 0;
        this.batchSize = batchSize;
        this.baseRate = baseRate;
        this.resolution = resolution;
        this.elements = new LinkedBlockingQueue<>();
        this.numberOfRecordsPerTask = batchSize;

        this.newBatchSemaphore = new Semaphore(1);
    }

    public void enableBusyWait() {
        this.busyWait = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        taskNumber = getRuntimeContext().getIndexOfThisSubtask();

        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        resolutionPerTask = ((double) resolution) / parallelism;
        double baseRatePerTask = ((double) baseRate) / parallelism;
        numberOfRecordsPerTask = batchSize / parallelism;

        if (taskNumber == 0) {
            // getting remaining records, if any
            numberOfRecordsPerTask += batchSize % parallelism;
        }

        currentRate = baseRatePerTask;
        updateWaitPeriod();

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.observe(this);

        Address address = jobControlClient.discoverServer(trackingServerNameForDiscovery);
        requestTrackerClient = new StringClient(address.ip, address.port);
        requestTrackerClient.init();

        trackerThread = new Thread(new Tracker());
        trackerThread.setName("Tracker for " + Thread.currentThread().getName());
        trackerThread.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
        requestTrackerClient.close();

        trackerThread.join();
    }

    private void updateWaitPeriod() {
        this.waitPeriodMicro = (long) (Math.pow(10, 6) / currentRate);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

        Optional<T> out;
        do {
            out = elements.take();
            out.ifPresent(sourceContext::collect);
        } while (out.isPresent());
    }

    protected abstract T getNext(int count);

    @Override
    public void cancel() {
        this.stop = true;
    }

    @Override
    public void onJobFinish() {
        JobControlListener.super.onJobFinish();
        cancel();
        trackerThread.interrupt();
    }

    @Override
    public void onBatchEnd() {
        currentRate += resolutionPerTask;
        updateWaitPeriod();
        newBatchSemaphore.release();
    }

    private class Tracker implements Runnable {

        @Override
        public void run() {
            try {
                String prefix = "TunableSource task " + taskNumber + " - ";

                while (!stop) {
                    newBatchSemaphore.acquire();

                    String batchDescription = "total-size: " + batchSize + "[records], " +
                            "local-size: " + numberOfRecordsPerTask + "[records], " +
                            "local-rate: " + currentRate + "[records/s]";

                    LOG.info(prefix + "Starting with batch: " + batchDescription);
                    do {
                        T next = getNext(count);
                        elements.add(Optional.of(next));
                        requestTrackerClient.send(next.getUniqueRepresentation());
                        count++;

                        sleep();
                    } while (!stop && count % numberOfRecordsPerTask != 0);
                    LOG.info(prefix + "Finished with batch: " + batchDescription);
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted: " + e.getMessage());
            } finally {
                elements.add(Optional.empty());
            }
        }

        private void sleep() throws InterruptedException {
            if (busyWait) {
                long start = System.nanoTime();
                while (System.nanoTime() - start < waitPeriodMicro * 1000) {
                    // busy loop
                }
            } else {
                TimeUnit.MICROSECONDS.sleep(waitPeriodMicro);
            }
        }
    }

    public static class TunableTransferSource extends TunableSource<TransferID> {

        public TunableTransferSource(int baseRate, int resolution, int batchSize, String trackingServerNameForDiscovery) {
            super(baseRate, resolution, batchSize, trackingServerNameForDiscovery);
        }

        @Override
        protected TransferID getNext(int count) {
            return new TransferID(taskNumber, (long) count);
        }
    }

    public static class TunableQuerySource extends TunableSource<Query> {
        private transient QuerySupplier supplier;
        private final int keyspaceSize, averageQuerySize;
        private final String namespace;
        private int stdDevQuerySize;

        public TunableQuerySource(
                int baseRate, int resolution, int batchSize, String trackingServerName, String namespace,
                int keyspaceSize, int averageQuerySize, int stdDevQuerySize) {
            super(baseRate, resolution, batchSize, trackingServerName);
            this.keyspaceSize = keyspaceSize;
            this.averageQuerySize = averageQuerySize;
            this.stdDevQuerySize = stdDevQuerySize;
            this.namespace = namespace;
        }

        @Override
        protected Query getNext(int count) {
            if (supplier == null) {
                supplier = new RandomQuerySupplier(
                        namespace, taskNumber, Transfer.KEY_PREFIX, keyspaceSize, averageQuerySize, stdDevQuerySize);
            }

            return supplier.getQuery(new QueryID(taskNumber, (long) count));
        }
    }

    public static class TunableSPUSource extends TunableSource<SinglePartitionUpdate> {
        private transient RandomSPUSupplier supplier;
        private final String namespace;
        private final int keyspaceSize;
        private final List<SinglePartitionUpdate.Command<?>> commands;

        public TunableSPUSource(
                int baseRate, int resolution, int batchSize, String trackingServerNameForDiscovery,
                String namespace, int keyspaceSize) {
            super(baseRate, resolution, batchSize, trackingServerNameForDiscovery);
            this.namespace = namespace;
            this.keyspaceSize = keyspaceSize;
            this.commands = new LinkedList<>();
        }

        public void addCommand(SinglePartitionUpdate.Command<?> command) {
            commands.add(command);
        }

        @Override
        protected SinglePartitionUpdate getNext(int count) {
            if (commands.isEmpty()) {
                throw new RuntimeException("Provide commands please");
            }

            if (supplier == null) {
                supplier = new RandomSPUSupplier(namespace, taskNumber, Transfer.KEY_PREFIX,
                        keyspaceSize, commands);
            }

            return supplier.next(new SinglePartitionUpdateID(taskNumber, (long) count));
        }
    }


    public static class ToTransfers extends RichMapFunction<TransferID, Transfer> {
        private transient Random random;
        private final int noAccounts;
        private final double startAmount;

        public ToTransfers(int noAccounts, double startAmount) {
            this.noAccounts = noAccounts;
            this.startAmount = startAmount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            random = new Random(getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Transfer map(TransferID tid) throws Exception {
            return Transfer.generateTransfer(tid, noAccounts, startAmount, random);
        }
    }
}
