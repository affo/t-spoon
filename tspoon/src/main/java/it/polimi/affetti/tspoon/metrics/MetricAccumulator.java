package it.polimi.affetti.tspoon.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * Created by affo on 31/03/17.
 */
public class MetricAccumulator implements Accumulator<Double, Metric> {
    private final Metric metric;

    public MetricAccumulator() {
        this(new Metric());
    }

    public MetricAccumulator(Metric metric) {
        this.metric = metric;
    }

    @Override
    public void add(Double val) {
        metric.add(val);
    }

    @Override
    public Metric getLocalValue() {
        return metric;
    }

    @Override
    public void resetLocal() {
        metric.reset();
    }

    @Override
    public void merge(Accumulator<Double, Metric> accumulator) {
        double[] values = accumulator.getLocalValue().metric.getValues();
        for (double v : values) {
            add(v);
        }
    }

    @Override
    public Accumulator<Double, Metric> clone() {
        MetricAccumulator cloned = new MetricAccumulator();
        cloned.merge(this);
        return cloned;
    }
}
