package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.metrics.Metric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 01/08/17.
 */
public class TimestampDeltaServer extends ProcessRequestServer {
    private Map<String, Long> beginTimestamps = new HashMap<>();
    private Map<String, Long> endTimestamps = new HashMap<>();
    private Map<String, Metric> metrics = new HashMap<>();

    public Map<String, Metric> getMetrics() {
        return metrics;
    }

    @Override
    protected synchronized void parseRequest(String request) {
        // something like >latency.1
        boolean isBegin = request.startsWith(">");
        String key = request.substring(1);
        long timestamp = System.currentTimeMillis();

        String metricName = key.split("\\.")[0];
        Metric metric = metrics.computeIfAbsent(metricName, k -> new Metric());

        if (isBegin) {
            beginTimestamps.put(key, timestamp);
        } else {
            endTimestamps.put(key, timestamp);
        }

        Long begin = beginTimestamps.get(key);
        Long end = endTimestamps.get(key);

        if (begin != null && end != null) {
            long delta = end - begin;
            if (delta >= 0) {
                metric.add((double) delta);
            } else {
                LOG.warn("Negative timestamp calculated. This means that end came before beginning...");
            }
        }

    }
}
