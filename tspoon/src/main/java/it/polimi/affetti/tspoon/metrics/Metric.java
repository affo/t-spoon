package it.polimi.affetti.tspoon.metrics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 31/03/17.
 */
public class Metric implements Serializable {
    public final DescriptiveStatistics metric = new DescriptiveStatistics();

    public void add(Double d) {
        metric.addValue(d);
    }

    @Override
    public String toString() {
        return toJSON().toString();
    }

    public JSONObject toJSON() {
        return new JSONObject(toMap());
    }

    public Map<String, Double> toMap() {
        Map<String, Double> res = new HashMap<>();
        res.put("n", (double) metric.getN());
        res.put("mean", metric.getMean());
        res.put("mode", metric.getPercentile(50));
        res.put("5th", metric.getPercentile(5));
        res.put("95th", metric.getPercentile(95));
        res.put("stddev", metric.getStandardDeviation());
        return res;
    }
}
