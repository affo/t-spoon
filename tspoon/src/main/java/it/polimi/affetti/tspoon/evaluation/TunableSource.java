package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.MetricCurveAccumulator;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by affo on 26/07/17.
 * <p>
 * Sends the ping to keep a rate at source.
 */
public abstract class TunableSource<T extends UniquelyRepresentableForTracking>
        extends RichSourceFunction<T> {
    public static final long ABORT_TIMEOUT = 3 * 60 * 1000;
    public static final String TARGETING_CURVE_ACC = "targeting-curve";
    public static final String REFINING_CURVE_ACC = "refining-curve";
    public static final String LATENCY_UNLOADED_ACC = "latency-unloaded";
    public static final String THROUGHPUT_ACC = "throughput";
    public static final String BACKPRESSURE_THROUGHPUT_ACC = "backpressure-throughput";

    protected transient Logger LOG;
    protected int taskNumber = 0; // for portability in case of parallel source

    private transient Timer timer;
    private boolean busyWait = true;
    private final int numberOfSamplesUnloaded, minAveragingSteps, maxAveragingSteps;
    private final long backPressureBatchSize, unloadedBatchSize, targetingBatchSize;
    protected int count;
    private int recordsInQueue;
    private double resolution, stdDevLimit;
    private volatile boolean batchOpen = true;
    private transient MetricCalculator metricCalculator;

    // accumulators
    private final MetricCurveAccumulator targetingCurve, averagingCurve;
    private final MetricAccumulator latencyUnloaded, throughput, backpressureThroughput;

    private transient JobControlClient jobControlClient;
    private transient WithServer requestTracker;
    private final String trackingServerNameForDiscovery;

    public TunableSource(EvalConfig config, String trackingServerNameForDiscovery) {
        this.backPressureBatchSize = config.backPressureBatchSize;
        this.unloadedBatchSize = config.unloadedBatchSize;
        this.targetingBatchSize = config.targetingBatchSize;
        this.recordsInQueue = config.recordsInQueue;
        this.numberOfSamplesUnloaded = config.numberOfSamplesUnloaded;
        this.minAveragingSteps = config.minAveragingSteps;
        this.maxAveragingSteps = config.maxAveragingSteps;
        this.stdDevLimit = config.stdDevLimit;
        this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
        this.count = 0;
        this.resolution = config.resolution;

        this.targetingCurve = new MetricCurveAccumulator();
        this.averagingCurve = new MetricCurveAccumulator();
        this.latencyUnloaded = new MetricAccumulator();
        this.backpressureThroughput = new MetricAccumulator();
        this.throughput = new MetricAccumulator();
    }

    public void disableBusyWait() {
        this.busyWait = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger("TunableSource");

        timer = new Timer("BatchingTimer");
        metricCalculator = new MetricCalculator();

        getRuntimeContext().addAccumulator(TARGETING_CURVE_ACC, targetingCurve);
        getRuntimeContext().addAccumulator(REFINING_CURVE_ACC, averagingCurve);
        getRuntimeContext().addAccumulator(LATENCY_UNLOADED_ACC, latencyUnloaded);
        getRuntimeContext().addAccumulator(THROUGHPUT_ACC, throughput);
        getRuntimeContext().addAccumulator(BACKPRESSURE_THROUGHPUT_ACC, backpressureThroughput);

        requestTracker = new WithServer(new TrackingServer());
        requestTracker.open();

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.registerServer(trackingServerNameForDiscovery, requestTracker.getMyAddress());
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
        requestTracker.close();
    }

    private void startBatch(long lengthMilliseconds) {
        batchOpen = true;
        metricCalculator.start();
        TimerTask batchTask = new TimerTask() {
            @Override
            public void run() {
                batchOpen = false;
            }
        };
        timer.schedule(batchTask, lengthMilliseconds);
    }

    private SourceContext<T> context;

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        this.context = sourceContext;

        runInitPhase();

        double latencyUnloaded = getLatencyUnloaded();
        double latencyThreshold = latencyUnloaded * recordsInQueue;
        LOG.info(">>> Latency unloaded is " + latencyUnloaded);
        LOG.info(">>> Latency threshold is " + latencyThreshold);
        double maxThroughput = runBackpressurePhase();

        if (maxThroughput < resolution) {
            resolution = maxThroughput / 2;
        }

        double sustainableThroughput = runTargetingPhase(maxThroughput, latencyThreshold);
        runAveragingPhase(sustainableThroughput, latencyThreshold);

        LOG.info(">>> Evaluation completed, sustainable throughput is " + throughput.getLocalValue().metric.getMean());
        jobControlClient.publishFinishMessage();
    }

    private void runInitPhase() throws TimeoutException, InterruptedException {
        LOG.info(">>> Sending init batches");
        // a pair of batches, just to be sure that every connection is set
        launchUnloadedBatch();
        launchUnloadedBatch();
    }

    private double getLatencyUnloaded() throws TimeoutException, InterruptedException {
        LOG.info(">>> Latency unloaded phase");
        for (int i = 0; i < numberOfSamplesUnloaded; i++) {
            LOG.info(">>> Sending unloaded batch [" + (i + 1) + "/" + numberOfSamplesUnloaded + "]");
            MetricCalculator.Measurement mUnloaded = launchUnloadedBatch();
            LOG.info(">>> Unloaded data: " + mUnloaded);
            latencyUnloaded.add(mUnloaded.latency);
        }
        return latencyUnloaded.getLocalValue().metric.getMean();
    }

    private double runBackpressurePhase() throws TimeoutException, InterruptedException {
        LOG.info(">>> Backpressure phase, sending at maximum rate");
        MetricCalculator.Measurement mMax = launchMaxBatch();
        backpressureThroughput.add(mMax.throughput);
        LOG.info(">>> Overloaded data: " + mMax);
        return mMax.throughput;
    }

    private double runTargetingPhase(double maxThroughput, double latencyThreshold) throws InterruptedException {
        LOG.info(">>> Starting targeting phase with target latency " + latencyThreshold + "[ms]");
        double currentLatency, currentRate = maxThroughput;
        do {
            MetricCalculator.Measurement m = launchBatch(currentRate, targetingBatchSize);

            if (!m.isValid()) {
                // the system is overloaded
                break;
            }

            currentLatency = m.latency;
            currentRate += resolution * 2;
            maxThroughput = Math.max(maxThroughput, m.throughput);

            LOG.info(">>> Targeting data: " + m);
            targetingCurve.add(Point.of(m.inputRate, currentRate, m.latency, m.throughput));
        } while (currentLatency < latencyThreshold);

        return maxThroughput;
    }

    private void runAveragingPhase(double sustainableThroughput, double latencyThreshold) throws InterruptedException {
        LOG.info(">>> Starting averaging phase with sustainable throughput " + sustainableThroughput + "[r/s]");
        double stdDevPercentage = 1;
        int step = 0;
        while ((step < minAveragingSteps || stdDevPercentage > stdDevLimit) && step < maxAveragingSteps) {
            MetricCalculator.Measurement m = launchOneSecondBatch(sustainableThroughput);

            if (!m.isValid() || m.latency >= latencyThreshold) {
                LOG.info(">>> The latency is above threshold, rate -= " + resolution);
                sustainableThroughput -= resolution;
                if (sustainableThroughput <= 0) {
                    sustainableThroughput = 1;
                }
            } else {
                LOG.info(">>> The latency is below threshold, rate += " + resolution);
                sustainableThroughput += resolution;
            }

            LOG.info(">>> Data: " + m);
            averagingCurve.add(Point.of(m.inputRate, sustainableThroughput, m.latency, m.throughput));
            throughput.add(m.throughput);
            stdDevPercentage = throughput.getLocalValue().metric.getStandardDeviation() / m.throughput;
            step++;
        }
    }

    private MetricCalculator.Measurement launchOneSecondBatch(double rate) throws InterruptedException {
        return launchBatch(rate, 1000);
    }

    private MetricCalculator.Measurement launchBatch(double rate, long size) throws InterruptedException {
        startBatch(size);
        emitRecordsAtRate(rate);
        return metricCalculator.end();
    }

    private MetricCalculator.Measurement launchUnloadedBatch() throws InterruptedException, TimeoutException {
        startBatch(unloadedBatchSize);
        emitRecordsOneByOne();
        return metricCalculator.end();
    }

    private MetricCalculator.Measurement launchMaxBatch() throws InterruptedException, TimeoutException {
        startBatch(backPressureBatchSize);
        emitRecordsAtMaxRate();
        return metricCalculator.endAfterCompletion(ABORT_TIMEOUT);
    }

    private void emitRecordsAtMaxRate() {
        while (batchOpen) {
            T next = getNext(count++);
            metricCalculator.sent(next.getUniqueRepresentation());
            context.collect(next);
        }
    }

    private void emitRecordsAtRate(double currentRate) throws InterruptedException {
        long waitPeriodMicro = (long) (Math.pow(10, 6) / currentRate);
        long start = System.nanoTime();
        long localCount = 0;

        while (batchOpen) {
            T next = getNext(count++);

            metricCalculator.sent(next.getUniqueRepresentation());
            context.collect(next);
            localCount++;

            long timeForNext = waitPeriodMicro * localCount;
            long deltaFromBeginning = (long) ((System.nanoTime() - start) * Math.pow(10, -3));
            long stillToSleep = timeForNext - deltaFromBeginning;

            sleep(stillToSleep);
        }
    }

    private void emitRecordsOneByOne() throws InterruptedException, TimeoutException {
        while (batchOpen) {
            T next = getNext(count++);
            metricCalculator.sent(next.getUniqueRepresentation());
            context.collect(next);
            metricCalculator.waitForEveryRecordUntilThisPoint(ABORT_TIMEOUT);
        }
    }

    private void sleep(long stillToSleep) throws InterruptedException {
        if (stillToSleep < 0) {
            return;
        }

        if (busyWait) {
            long start = System.nanoTime();
            while (System.nanoTime() - start < stillToSleep * 1000) {
                // busy loop B)
            }
        } else {
            TimeUnit.MICROSECONDS.sleep(stillToSleep);
        }
    }

    protected abstract T getNext(int count);

    @Override
    public void cancel() {
    }

    private class TrackingServer extends ProcessRequestServer {
        /**
         * The input string must be a UniqueRepresentation
         * @param recordID
         */
        @Override
        protected void parseRequest(String recordID) {
            metricCalculator.received(recordID);
        }
    }

    private static class Point extends MetricCurveAccumulator.Point {
        public final double actualRate, expectedRate;
        public final double latency, throughput;

        public Point(double actualRate, double expectedRate, double latency, double throughput) {
            this.actualRate = actualRate;
            this.expectedRate = expectedRate;
            this.latency = latency;
            this.throughput = throughput;
        }

        public static Point of(double actualRate, double expectedRate, double latency, double throughput) {
            return new Point(actualRate, expectedRate, latency, throughput);
        }

        @Override
        public JSONObject toJSON() {
            Map<String, Object> map = new HashMap<>();
            map.put("actualRate", actualRate);
            map.put("expectedRate", expectedRate);
            map.put("latency", latency);
            map.put("throughput", throughput);
            return new JSONObject(map);
        }
    }
}
