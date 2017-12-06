package it.polimi.affetti.tspoon.metrics;

import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 31/03/17.
 */
public class DynamicMetric implements Serializable {
    public final HashMap<String, Metric> metrics = new HashMap<>();

    public void add(String key, Double d) {
        metrics.computeIfAbsent(key, k -> new Metric()).add(d);
    }

    public void clear() {
        metrics.clear();
    }

    @Override
    public String toString() {
        Map<String, JSONObject> mapped = new HashMap<>();
        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            mapped.put(entry.getKey(), entry.getValue().toJSON());
        }
        return new JSONObject(mapped).toString();
    }
}
