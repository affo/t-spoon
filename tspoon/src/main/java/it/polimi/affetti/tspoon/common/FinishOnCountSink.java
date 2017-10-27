package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

/**
 * Created by affo on 26/07/17.
 */
public class FinishOnCountSink<T> extends RichSinkFunction<T> {
    private transient Logger LOG;
    public static final int DEFAULT_LOG_EVERY = 10000;
    private final int logEvery;

    private transient JobControlClient jobControlClient;
    private final int threshold;
    private int count;

    public FinishOnCountSink(int count) {
        this(count, DEFAULT_LOG_EVERY);
    }

    public FinishOnCountSink(int count, int logEvery) {
        this.count = 0;
        this.threshold = count;
        this.logEvery = logEvery;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(FinishOnCountSink.class.getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jobControlClient != null) {
            jobControlClient.close();
        }
    }

    @Override
    public void invoke(T t) throws Exception {
        if (count % logEvery == 0) {
            LOG.info((threshold - count) + " records remaining");
        }

        count++;

        if (count > threshold) {
            throw new RuntimeException("Counted too much: " + count);
        }

        if (jobControlClient != null && count == threshold) {
            jobControlClient.publish(JobControlClient.finishPattern);
        }
    }
}
