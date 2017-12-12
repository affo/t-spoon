package it.polimi.affetti.tspoon.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.sling.commons.json.JSONObject;
import scala.Serializable;

import java.util.LinkedList;

/**
 * Created by affo on 12/12/17.
 */
public class MetricCurveAccumulator
        implements Accumulator<MetricCurveAccumulator.Point, LinkedList<MetricCurveAccumulator.Point>> {
    private final LinkedList<Point> points = new LinkedList<>();

    @Override
    public void add(Point point) {
        points.add(point);
    }

    @Override
    public LinkedList<Point> getLocalValue() {
        return points;
    }

    @Override
    public void resetLocal() {
        points.clear();
    }

    @Override
    public void merge(Accumulator<Point, LinkedList<Point>> accumulator) {
        points.addAll(accumulator.getLocalValue());
    }

    @Override
    public Accumulator<Point, LinkedList<Point>> clone() {
        MetricCurveAccumulator cloned = new MetricCurveAccumulator();
        for (Point point : points) {
            cloned.add(point);
        }
        return cloned;
    }

    public abstract static class Point implements Serializable {
        public abstract JSONObject toJSON();

        @Override
        public String toString() {
            return toJSON().toString();
        }
    }
}
