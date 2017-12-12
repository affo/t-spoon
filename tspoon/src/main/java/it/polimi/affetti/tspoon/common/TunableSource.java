package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.JobControlListener;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 26/07/17.
 * <p>
 * Sends the ping to keep a rate at source.
 */
public class TunableSource extends RichParallelSourceFunction<Object> implements JobControlListener {
    protected transient Logger LOG;

    private final int baseRate, resolution, batchSize;
    private int count, numberOfRecordsPerTask;
    private double resolutionPerTask, currentRate;

    private int taskNumber;
    private volatile boolean stop;

    private Semaphore newBatchSemaphore;

    private transient JobControlClient jobControlClient;

    public TunableSource(int baseRate, int resolution, int batchSize) {
        this.count = 0;
        this.batchSize = batchSize;
        this.baseRate = baseRate;
        this.resolution = resolution;

        this.numberOfRecordsPerTask = batchSize;

        this.newBatchSemaphore = new Semaphore(1);
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

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.observe(this);
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    private long getWaitPeriodInMicroseconds() {
        return (long) (Math.pow(10, 6) / currentRate);
    }

    @Override
    public void run(SourceContext<Object> sourceContext) throws Exception {
        String prefix = "TunableSource task " + taskNumber + " - ";

        while (!stop) {
            newBatchSemaphore.acquire();

            String batchDescription = "total-size: " + batchSize + "[records], " +
                    "local-size: " + numberOfRecordsPerTask + "[records], " +
                    "local-rate: " + currentRate + "[records/s]";

            LOG.info(prefix + "Starting with batch: " + batchDescription);
            while (!stop && count < numberOfRecordsPerTask) {
                sourceContext.collect(0); // useless content
                count++;
                TimeUnit.MICROSECONDS.sleep(getWaitPeriodInMicroseconds());
            }
            LOG.info(prefix + "Finished with batch: " + batchDescription);

            count = 0;
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }

    @Override
    public void onJobFinish() {
        JobControlListener.super.onJobFinish();
        cancel();
        newBatchSemaphore.release();
    }

    @Override
    public void onBatchEnd() {
        currentRate += resolutionPerTask;
        newBatchSemaphore.release();
    }

    public static class ToTransfers implements MapFunction<Object, Transfer> {
        private int count = 0;
        private final int noAccounts;
        private final double startAmount;

        public ToTransfers(int noAccounts, double startAmount) {
            this.noAccounts = noAccounts;
            this.startAmount = startAmount;
        }

        @Override
        public Transfer map(Object useless) throws Exception {
            return Transfer.generateTransfer(count++, noAccounts, startAmount);
        }
    }
}
