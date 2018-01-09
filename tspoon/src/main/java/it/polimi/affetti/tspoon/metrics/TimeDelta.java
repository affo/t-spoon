package it.polimi.affetti.tspoon.metrics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 12/12/17.
 */
public class TimeDelta implements Serializable {
    private Map<String, Long> beginTimestamps = new HashMap<>();
    private Map<String, Long> endTimestamps = new HashMap<>();
    private Metric deltaStat = new Metric();

    /**
     * @param id
     * @return true if the value added generated a new entry for the underlying metric
     */
    public boolean start(String id) {
        beginTimestamps.put(id, System.currentTimeMillis());
        return addToMetric(id);
    }

    /**
     * @param id
     * @return true if the value added generated a new entry for the underlying metric
     */
    public boolean end(String id) {
        endTimestamps.put(id, System.currentTimeMillis());
        return addToMetric(id);
    }

    private boolean addToMetric(String id) {
        Double delta = removeDelta(id);
        if (delta == null || delta < 0) {
            return false;
        }

        deltaStat.add(delta);
        return true;
    }

    /**
     * Removes delta if a begin and start event for the specified id is specified.
     * <p>
     * Returns null and does not remove anything in the case a begin/start pair is not available.
     * Otherwise it returns the delta (could be negative...)
     *
     * @param id
     * @return
     */
    private Double removeDelta(String id) {
        Long begin = beginTimestamps.get(id);
        Long end = endTimestamps.get(id);
        Double delta = null;

        if (begin != null && end != null) {
            delta = (double) (end - begin);

            // when matching pair is found, remove
            beginTimestamps.remove(id);
            endTimestamps.remove(id);
        }

        return delta;
    }

    public Metric getMetric() {
        return deltaStat;
    }

    public Double getMeanValue() {
        return deltaStat.metric.getMean();
    }

    public MetricAccumulator getNewAccumulator() {
        return new MetricAccumulator(getMetric());
    }
}
