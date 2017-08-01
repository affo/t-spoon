package it.polimi.affetti.tspoon.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * Created by affo on 31/03/17.
 */
public class MetricAccumulator implements Accumulator<Double, Metric> {
    private final Metric metric;
    private final String name;

    public MetricAccumulator(String name) {
        this.name = name;
        this.metric = new Metric(name);
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
        metric.metric.clear();
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
        MetricAccumulator cloned = new MetricAccumulator(name);
        cloned.merge(this);
        return cloned;
    }
}
