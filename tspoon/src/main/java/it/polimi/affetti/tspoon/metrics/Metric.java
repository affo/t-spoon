package it.polimi.affetti.tspoon.metrics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 31/03/17.
 */
public class Metric implements Serializable {
    private final String prefix;
    public final DescriptiveStatistics metric = new DescriptiveStatistics();

    public Metric(String name) {
        this.prefix = name;
    }

    public void add(Double d) {
        metric.addValue(d);
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    public Map<String, Double> toMap() {
        Map<String, Double> res = new HashMap<>();
        res.put(prefix + "-n", (double) metric.getN());
        res.put(prefix + "-mean", metric.getMean());
        res.put(prefix + "-mode", metric.getPercentile(50));
        res.put(prefix + "-5th", metric.getPercentile(5));
        res.put(prefix + "-95th", metric.getPercentile(95));
        res.put(prefix + "-stddev", metric.getStandardDeviation());
        return res;
    }
}
