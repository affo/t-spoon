package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.metrics.DynamicMetricAccumulator;
import it.polimi.affetti.tspoon.metrics.Report;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 01/08/17.
 */
public class TimestampDeltaSink extends RichSinkFunction<Tuple3<String, Boolean, String>> {
    public static final String METRIC_ACC = "timestamp-deltas";

    private transient Logger LOG;
    private Map<String, Long> beginTimestamps = new HashMap<>();
    private Map<String, Long> endTimestamps = new HashMap<>();
    private DynamicMetricAccumulator metrics = new DynamicMetricAccumulator();

    public TimestampDeltaSink() {
        Report.registerAccumulator(METRIC_ACC);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(TimestampDeltaSink.class.getSimpleName());

        getRuntimeContext().addAccumulator(METRIC_ACC, metrics);
    }

    @Override
    public void invoke(Tuple3<String, Boolean, String> toTrack) throws Exception {
        String metricName = toTrack.f0;
        boolean isBegin = toTrack.f1;
        String key = toTrack.f2;
        long timestamp = System.currentTimeMillis();

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
                metrics.add(Tuple2.of(metricName, (double) delta));
            } else {
                LOG.warn("Negative timestamp calculated. This means that end came before beginning...");
            }

            // when matching pair is found, remove
            beginTimestamps.remove(key);
            endTimestamps.remove(key);
        }
    }
}
