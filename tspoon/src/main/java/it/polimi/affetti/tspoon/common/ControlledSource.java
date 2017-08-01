package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.JobControlListener;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.Semaphore;

/**
 * Created by affo on 26/07/17.
 */
public abstract class ControlledSource<T> extends RichSourceFunction<T> implements JobControlListener {
    private Semaphore jobFinish = new Semaphore(0);
    private transient JobControlClient jobControlClient;
    protected boolean stop;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        if (jobControlClient != null) {
            jobControlClient.observe(this);
        }
    }

    protected void waitForFinish() throws InterruptedException {
        jobFinish.acquire();
    }

    @Override
    public void cancel() {
        stop = true;
        jobControlClient.stop();
    }

    @Override
    public void onJobFinish() {
        jobFinish.release();
    }
}
