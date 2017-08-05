package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.runtime.TimestampDeltaClient;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 * Created by affo on 01/08/17.
 */
public abstract class TimestampTracker<T> extends RichMapFunction<T, T> {
    private transient TimestampDeltaClient timestampDeltaClient;
    private final String metricName;
    private final boolean isBegin;

    public TimestampTracker(String metricName, boolean isBegin) {
        this.metricName = metricName;
        this.isBegin = isBegin;
    }

    protected abstract String extractId(T element);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        timestampDeltaClient = TimestampDeltaClient.get(parameterTool);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (timestampDeltaClient != null) {
            timestampDeltaClient.close();
        }
    }

    private void track(T element) {
        String id = extractId(element);
        if (isBegin) {
            timestampDeltaClient.begin(metricName, id);
        } else {
            timestampDeltaClient.end(metricName, id);
        }
    }

    @Override
    public T map(T t) throws Exception {
        if (timestampDeltaClient != null) {
            track(t);
        }
        return t;
    }
}
