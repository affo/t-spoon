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

import java.util.Random;
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
    public static final double DISCARD_PERCENTAGE = 0.1;
    protected transient Logger LOG;

    private boolean busyWait = false;
    protected final int baseRate, resolution, batchSize;
    protected int count, numberOfRecordsPerTask, skipFirst;
    protected double resolutionPerTask, currentRate;
    protected long waitPeriodMicro;
    private final String trackingServerNameForDiscovery;

    protected int taskNumber;
    protected volatile boolean stop;

    private Semaphore newBatchSemaphore;

    private transient JobControlClient jobControlClient;
    private transient StringClient requestTrackerClient;
    private double threshold;

    public TunableSource(int baseRate, int resolution, int batchSize,
                         double backPressureThreshold, String trackingServerNameForDiscovery) {
        this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
        this.count = 0;
        this.batchSize = batchSize;
        this.threshold = backPressureThreshold;
        this.baseRate = baseRate;
        this.resolution = resolution;
        this.numberOfRecordsPerTask = batchSize;

        this.newBatchSemaphore = new Semaphore(1);
    }

    public void enableBusyWait() {
        this.busyWait = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        LOG = Logger.getLogger("TunableSource-" + taskNumber);

        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        resolutionPerTask = ((double) resolution) / parallelism;
        double baseRatePerTask = ((double) baseRate) / parallelism;
        numberOfRecordsPerTask = batchSize / parallelism;

        if (taskNumber == 0) {
            // getting remaining records, if any
            numberOfRecordsPerTask += batchSize % parallelism;
        }

        skipFirst = (int) (numberOfRecordsPerTask * DISCARD_PERCENTAGE); // the first 10 percent is discarded

        currentRate = baseRatePerTask;
        updateWaitPeriod();

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.observe(this);

        Address address = jobControlClient.discoverServer(trackingServerNameForDiscovery);
        requestTrackerClient = new StringClient(address.ip, address.port);
        requestTrackerClient.init();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
        requestTrackerClient.close();
    }

    private void updateWaitPeriod() {
        this.waitPeriodMicro = (long) (Math.pow(10, 6) / currentRate);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

        try {
            while (!stop) {
                newBatchSemaphore.acquire();
                int loopLocalCount = 0;
                double totalLatency = 0.0;

                String batchDescription = "total-size: " + batchSize + "[records], " +
                        "local-size: " + numberOfRecordsPerTask + "[records], " +
                        "local-rate: " + currentRate + "[records/s]";

                LOG.info("Starting with batch: " + batchDescription);
                do {
                    T next = getNext(count);

                    long start = System.nanoTime(), afterSend = start, afterCollect;
                    if (loopLocalCount + 1 > skipFirst) {
                        // don't send the initial (per-batch) sled, the metric calculator
                        // will only track useful records
                        requestTrackerClient.send(next.getUniqueRepresentation());
                        afterSend = System.nanoTime();
                    }

                    sourceContext.collect(next);
                    afterCollect = System.nanoTime();

                    // calculate delta in milliseconds, detecting backPressure
                    double delta = (afterCollect - afterSend) / Math.pow(10, 6);

                    if (loopLocalCount + 1 > skipFirst) {
                        totalLatency += delta;
                    }

                    sleep((long) ((afterCollect - start) / Math.pow(10, 3)));
                    count++;
                    loopLocalCount++;
                } while (!stop && loopLocalCount < numberOfRecordsPerTask);
                LOG.info("Finished with batch: " + batchDescription);

                double avgLatency = totalLatency / (loopLocalCount - skipFirst);
                if (avgLatency >= threshold) {
                    LOG.info("Threshold exceeded (" + avgLatency + " > " + threshold + "), finishing...");
                    jobControlClient.publishFinishMessage();
                    return;
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted: " + e.getMessage());
        }
    }

    private void sleep(long alreadyElapsedMicro) throws InterruptedException {
        long stillToSleep = waitPeriodMicro - alreadyElapsedMicro;

        if (busyWait) {
            long start = System.nanoTime();
            while (System.nanoTime() - start < stillToSleep * 1000) {
                // busy loop
            }
        } else {
            if (stillToSleep > 0) {
                TimeUnit.MICROSECONDS.sleep(stillToSleep);
            }
        }
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
    }

    @Override
    public void onBatchEnd() {
        currentRate += resolutionPerTask;
        updateWaitPeriod();
        newBatchSemaphore.release();
    }

    public static class TunableTransferSource extends TunableSource<TransferID> {

        public TunableTransferSource(int baseRate, int resolution, int batchSize,
                                     double backPressureThreshold, String trackingServerNameForDiscovery) {
            super(baseRate, resolution, batchSize, backPressureThreshold, trackingServerNameForDiscovery);
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
                int baseRate, int resolution, int batchSize, double backPressureThreshold,
                String trackingServerName, String namespace,
                int keyspaceSize, int averageQuerySize, int stdDevQuerySize) {
            super(baseRate, resolution, batchSize, backPressureThreshold, trackingServerName);
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
        private RandomSPUSupplier supplier;
        private Random random;

        public TunableSPUSource(
                int baseRate, int resolution, int batchSize,
                double backPressureThreshold, String trackingServerNameForDiscovery, RandomSPUSupplier supplier) {
            super(baseRate, resolution, batchSize, backPressureThreshold, trackingServerNameForDiscovery);
            this.supplier = supplier;
        }

        @Override
        protected SinglePartitionUpdate getNext(int count) {
            if (random == null) {
                random = new Random(taskNumber);
            }

            return supplier.next(new SinglePartitionUpdateID(taskNumber, (long) count), random);
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
