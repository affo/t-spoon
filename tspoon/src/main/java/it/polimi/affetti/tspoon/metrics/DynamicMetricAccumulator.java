package it.polimi.affetti.tspoon.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

/**
 * Created by affo on 06/12/17.
 */
public class DynamicMetricAccumulator implements Accumulator<Tuple2<String, Double>, DynamicMetric> {
    private final DynamicMetric metrics = new DynamicMetric();

    @Override
    public void add(Tuple2<String, Double> val) {
        metrics.add(val.f0, val.f1);
    }

    @Override
    public DynamicMetric getLocalValue() {
        return metrics;
    }

    @Override
    public void resetLocal() {
        metrics.clear();
    }

    @Override
    public void merge(Accumulator<Tuple2<String, Double>, DynamicMetric> accumulator) {
        for (Map.Entry<String, Metric> entry : accumulator.getLocalValue().metrics.entrySet()) {
            String metricName = entry.getKey();
            Metric metric = entry.getValue();
            double[] values = metric.metric.getValues();
            for (double v : values) {
                add(Tuple2.of(metricName, v));
            }
        }
    }

    @Override
    public Accumulator<Tuple2<String, Double>, DynamicMetric> clone() {
        DynamicMetricAccumulator cloned = new DynamicMetricAccumulator();
        cloned.merge(this);
        return cloned;
    }
}
