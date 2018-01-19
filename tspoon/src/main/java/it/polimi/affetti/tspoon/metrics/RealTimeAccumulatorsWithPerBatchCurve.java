package it.polimi.affetti.tspoon.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by affo on 18/01/18.
 *
 * Keeps and registers a set of accumulators and a corresponding curve.
 * You can add a point to a curve and reset every accumulator by calling `addPoint`.
 * The Point will consist of:
 *  - as x: the batchNumber (incremental)
 *  - as y: a Map of every value of registered accumulators
 *
 * NOTE: The accumulators must be updated with external references (side-effect)
 */
public class RealTimeAccumulatorsWithPerBatchCurve implements Serializable {
    private final MetricCurveAccumulator theCurve;
    private Map<String, Accumulator<?, ?>> accumulators = new HashMap<>();
    private int batchNumber = 0;

    public RealTimeAccumulatorsWithPerBatchCurve() {
        this.theCurve = new MetricCurveAccumulator();
    }

    public void register(RuntimeContext context, String accumulatorName, Accumulator<?, ?> accumulator) {
        context.addAccumulator(accumulatorName, accumulator);
        accumulators.put(accumulatorName, accumulator);
    }

    public void registerCurve(RuntimeContext context, String curveName) {
        context.addAccumulator(curveName, theCurve);
    }

    public void addPoint() {
        Map<String, Object> y = accumulators.entrySet().stream()
                .map(entry -> {
                    String accumulatorName = entry.getKey();
                    Accumulator<?, ?> accumulator = entry.getValue();

                    Object value;
                    if (accumulator instanceof MetricAccumulator) {
                        value = ((MetricAccumulator) accumulator).getLocalValue().metric.getMean();
                    } else {
                        value = accumulator.getLocalValue();
                    }
                    accumulator.resetLocal();

                    return Tuple2.of(accumulatorName, value);
                })
                .collect(Collectors.toMap(t2 -> t2.f0, t2 -> t2.f1));

        theCurve.add(Point.of(batchNumber, y));
        batchNumber++;
    }

    private static class Point extends MetricCurveAccumulator.Point {
        public final int batchNumber;
        public final Map<String, Object> values;

        public Point(int batchNumber, Map<String, Object> values) {
            this.batchNumber = batchNumber;
            this.values = values;
        }

        public static Point of(int batchNumber, Map<String, Object> values) {
            return new Point(batchNumber, values);
        }

        @Override
        public JSONObject toJSON() {
            Map<String, Object> map = new HashMap<>();
            map.put("batchNumber", batchNumber);
            map.put("value", new JSONObject(values));
            return new JSONObject(map);
        }
    }
}
