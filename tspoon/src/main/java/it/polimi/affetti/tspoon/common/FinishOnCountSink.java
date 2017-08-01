package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 26/07/17.
 */
public class FinishOnCountSink<T> extends RichSinkFunction<T> {
    private transient JobControlClient jobControlClient;
    private int count;

    public FinishOnCountSink(int count) {
        this.count = count;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
    }

    @Override
    public void invoke(T t) throws Exception {
        count--;

        if (count < 0) {
            throw new Exception("Finish on count negative: " + count);
        }

        if (jobControlClient != null && count == 0) {
            jobControlClient.publish(JobControlClient.finishPattern);
        }
    }
}
