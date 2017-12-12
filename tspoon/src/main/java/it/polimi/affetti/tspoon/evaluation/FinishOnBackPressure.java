package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.TunableSource;
import it.polimi.affetti.tspoon.metrics.*;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 07/12/17.
 */
public class FinishOnBackPressure extends RichSinkFunction<Tuple2<Long, Boolean>> {
    public static final int SUB_BATCH_SIZE = 1000;
    public static final String THROUGHPUT_CURVE_ACC = "throughput-curve";
    public static final String LATENCY_CURVE_ACC = "latency-curve";
    public static final String MAX_TP_AND_LATENCY_ACC = "max-throughput-and-latency";
    private transient Logger LOG;

    private transient JobControlClient jobControlClient;
    private final int batchSize, resolution;
    public int expectedInputRate;
    private final double tolerance;
    private final MetricCurveAccumulator throughputCurve, latencyCurve;
    private TimeDelta currentLatency;
    private Throughput currentInputRate, currentThroughput;

    private ThroughputAndLatency maxThroughputAndLatency = new ThroughputAndLatency();

    public FinishOnBackPressure(double tolerance, int batchSize, int startInputRate, int resolution) {
        this.resolution = resolution;
        this.tolerance = tolerance;
        this.batchSize = batchSize;
        this.throughputCurve = new MetricCurveAccumulator();
        this.latencyCurve = new MetricCurveAccumulator();
        resetMetrics();

        this.expectedInputRate = startInputRate;

        Report.registerAccumulator(THROUGHPUT_CURVE_ACC);
        Report.registerAccumulator(LATENCY_CURVE_ACC);
        Report.registerAccumulator(MAX_TP_AND_LATENCY_ACC);
    }

    private void resetMetrics() {
        currentInputRate = new Throughput(batchSize, SUB_BATCH_SIZE);
        currentThroughput = new Throughput(batchSize, SUB_BATCH_SIZE);
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
        getRuntimeContext().addAccumulator(MAX_TP_AND_LATENCY_ACC, new SingleValueAccumulator<>(maxThroughputAndLatency));
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    @Override
    public void invoke(Tuple2<Long, Boolean> tracking) throws Exception {
        long id = tracking.f0;
        if (tracking.f1) {
            start(id);
        } else {
            end(id);
        }
    }

    private void start(long id) {
        currentLatency.start(id);
        currentInputRate.add();
    }

    private void end(long id) {
        currentLatency.end(id);
        boolean batchClosed = currentThroughput.add();

        if (batchClosed) {
            closeBatch();
        }
    }

    private void closeBatch() {
        double averageInputRate = currentInputRate.metric.getMean();
        double currentAverageThroughput = currentThroughput.metric.getMean();

        maxThroughputAndLatency.add(currentAverageThroughput, currentLatency.getMeanValue());

        throughputCurve.add(Point.of(averageInputRate, expectedInputRate, currentThroughput));
        latencyCurve.add(Point.of(averageInputRate, expectedInputRate, currentLatency.getMetric()));

        LOG.info("Batch of " + batchSize + " records @ " + averageInputRate + "[records/sec] closed: " +
                "avgThroughput: " + currentAverageThroughput + ", avgLatency: " + currentLatency.getMeanValue());

        if (averageInputRate < expectedInputRate - tolerance) {
            LOG.info("Actual input rate is far from expected: ([actual] " + averageInputRate +
                    " < [expected] " + expectedInputRate +
                    " - [tolerance]" + tolerance + "), finishing job.");
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
        public final Metric value;

        public Point(double actualRate, double expectedRate, Metric value) {
            this.actualRate = actualRate;
            this.expectedRate = expectedRate;
            this.value = value;
        }

        public static Point of(double actualRate, double expectedRate, Metric value) {
            return new Point(actualRate, expectedRate, value);
        }

        @Override
        public JSONObject toJSON() {
            Map<String, Object> map = new HashMap<>();
            map.put("actualRate", actualRate);
            map.put("expectedRate", expectedRate);
            map.put("value", value.toJSON());
            return new JSONObject(map);
        }
    }
}
