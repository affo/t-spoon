package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.*;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by affo on 07/12/17.
 */
public class FinishOnBackPressure<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
    public static final String THROUGHPUT_CURVE_ACC = "throughput-curve";
    public static final String LATENCY_CURVE_ACC = "latency-curve";
    public static final String MAX_TP_AND_LATENCY_ACC = "max-throughput-and-latency";
    private static final long ABORT_THRESHOLD = 60 * 1000; // [ms]
    private static final String abortExceptionMessage = "The job has entered a deadlock status. " +
            "This means that the sink hadn't ingested any record for " + ABORT_THRESHOLD + "[ms]. " +
            "This can be due to bugs in the code or to an excessive backpressure. " +
            "In the latter, results could be valid, but must be human validated.";
    private transient Logger LOG;

    private transient JobControlClient jobControlClient;
    private transient Timer timer;
    private transient TimerTask abortTask;
    private final String trackingServerName;
    private int countStart, countEnd, batchNumber, skipFirst, realBatchSize;
    private final int batchSize, resolution, maxNumberOfBatches;
    public int expectedInputRate;
    private final double errorPercentage;
    private final MetricCurveAccumulator throughputCurve, latencyCurve;
    private TimeDelta currentLatency;
    private Throughput currentInputRate, currentThroughput;

    private ThroughputAndLatency maxThroughputAndLatency = new ThroughputAndLatency();

    /*
    We send the begin of requests in a separate channel in order to avoid the BackPressure bias.
     */
    private transient WithServer requestTracker;

    public FinishOnBackPressure(double errorPercentage, int batchSize, int startInputRate,
                                int resolution, int maxNumberOfBatches, String trackingServerName) {
        if (errorPercentage < 0 || errorPercentage >= 1) {
            throw new IllegalArgumentException("Error Percentage must be a percentage: " + errorPercentage);
        }

        this.errorPercentage = errorPercentage;
        this.trackingServerName = trackingServerName;
        this.resolution = resolution;
        this.batchSize = batchSize;
        this.skipFirst = (int) (batchSize * (TunableSource.DISCARD_PERCENTAGE));
        this.realBatchSize = batchSize - skipFirst;
        this.maxNumberOfBatches = maxNumberOfBatches >= 1 ? maxNumberOfBatches : Integer.MAX_VALUE;
        this.throughputCurve = new MetricCurveAccumulator();
        this.latencyCurve = new MetricCurveAccumulator();
        resetMetrics();

        this.expectedInputRate = startInputRate;

        Report.registerAccumulator(THROUGHPUT_CURVE_ACC);
        Report.registerAccumulator(LATENCY_CURVE_ACC);
        Report.registerAccumulator(MAX_TP_AND_LATENCY_ACC);
    }

    private void resetMetrics() {
        // we calculate the inputRate only on starting records
        currentInputRate = new Throughput("ActualInputRate");
        // we calculate the throughput on every record (start and end) of the batch
        currentThroughput = new Throughput("Throughput");
        currentLatency = new TimeDelta();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(TunableSource.class.getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        getRuntimeContext().addAccumulator(THROUGHPUT_CURVE_ACC, throughputCurve);
        getRuntimeContext().addAccumulator(LATENCY_CURVE_ACC, latencyCurve);
        getRuntimeContext().addAccumulator(MAX_TP_AND_LATENCY_ACC,
                new SingleValueAccumulator<>(maxThroughputAndLatency));

        requestTracker = new WithServer(new TrackingServer());
        requestTracker.open();
        jobControlClient.registerServer(trackingServerName, requestTracker.getMyAddress());


        // ----------- init timer for abort
        timer = new Timer("AbortJobTimer");
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
        requestTracker.close();
        timer.cancel();
    }

    public String getTrackingServerName() {
        return trackingServerName;
    }

    @Override
    public void invoke(T tid) throws Exception {
        end(tid);
    }

    private TimerTask resetAbortTask() {
        if (abortTask != null) {
            abortTask.cancel();
        }

        timer.purge();
        abortTask = new TimerTask() {
            @Override
            public void run() {
                jobControlClient.terminateJobExceptionally(abortExceptionMessage);
            }
        };

        return abortTask;
    }

    private void scheduleAbortTask() {
        timer.schedule(resetAbortTask(), ABORT_THRESHOLD);
    }

    private synchronized void start(String id) {
        // no need to skip here, because the source doesn't send any record for latency tracking
        if (countStart == 0) {
            currentThroughput.open();
            currentInputRate.open();
        }

        countStart++;
        currentLatency.start(id);

        if (countStart % realBatchSize == 0) {
            currentInputRate.close(realBatchSize);
        }

        tryClose();
    }

    private synchronized void end(T id) {
        scheduleAbortTask();
        countEnd++;

        if (countEnd < skipFirst) {
            // skip the first record sent and not track them
            return;
        }

        currentLatency.end(id.getUniqueRepresentation());

        if (countStart == 0) {
            currentThroughput.open();
        }

        tryClose();
    }

    private void tryClose() {
        // the countStart doesn't count for skipped records
        // the countEnd counts every record
        if (countStart == realBatchSize && countEnd == batchSize) {
            currentThroughput.close(realBatchSize);
            closeBatch();
        }
    }

    private void closeBatch() {
        batchNumber++;

        countStart = 0;
        countEnd = 0;

        double averageInputRate = currentInputRate.getThroughput();
        double currentAverageThroughput = currentThroughput.getThroughput();

        maxThroughputAndLatency.add(currentAverageThroughput, currentLatency.getMeanValue());

        throughputCurve.add(Point.of(averageInputRate, expectedInputRate, currentAverageThroughput));
        latencyCurve.add(Point.of(averageInputRate, expectedInputRate, currentLatency.getMeanValue()));

        LOG.info("Batch of " + batchSize + " records @ " + averageInputRate + "[records/sec] closed: " +
                "avgThroughput: " + currentAverageThroughput + ", avgLatency: " + currentLatency.getMeanValue());

        boolean finish = false;
        if (batchNumber == maxNumberOfBatches) {
            LOG.info("Maximum number of batches reached: " + batchNumber);
            finish = true;
        }

        if (averageInputRate - currentAverageThroughput > averageInputRate * errorPercentage) {
            LOG.info("Actual input rate is far from throughput: ([actual] " + averageInputRate +
                    ", [throughput] " + currentAverageThroughput +
                    ", [tolerance]" + errorPercentage + "), finishing job.");
            finish = true;
        }

        if (finish) {
            jobControlClient.publishFinishMessage();
            return;
        }

        resetMetrics();
        expectedInputRate += resolution;
        jobControlClient.publishBatchEnd();
    }


    private static class ThroughputAndLatency implements Serializable {
        private double maxThroughput = 0, latency;

        public void add(double throughput, double latency) {
            if (throughput > maxThroughput) {
                this.maxThroughput = throughput;
                this.latency = latency;
            }
        }

        public double getMaxThroughput() {
            return maxThroughput;
        }

        @Override
        public String toString() {
            Map<String, Double> map = new HashMap<>();
            map.put("max-throughput", maxThroughput);
            map.put("latency-at-max-throughput", latency);
            return new JSONObject(map).toString();
        }
    }

    private static class Point extends MetricCurveAccumulator.Point {
        public final double actualRate, expectedRate;
        public final Double value;

        public Point(double actualRate, double expectedRate, Double value) {
            this.actualRate = actualRate;
            this.expectedRate = expectedRate;
            this.value = value;
        }

        public static Point of(double actualRate, double expectedRate, Double value) {
            return new Point(actualRate, expectedRate, value);
        }

        @Override
        public JSONObject toJSON() {
            Map<String, Object> map = new HashMap<>();
            map.put("actualRate", actualRate);
            map.put("expectedRate", expectedRate);
            map.put("value", value);
            return new JSONObject(map);
        }
    }

    private class TrackingServer extends ProcessRequestServer {
        /**
         * The input string must be a UniqueRepresentation
         * @param recordID
         */
        @Override
        protected void parseRequest(String recordID) {
            start(recordID);
        }
    }
}
