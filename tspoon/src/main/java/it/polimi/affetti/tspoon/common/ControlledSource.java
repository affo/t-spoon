package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.JobControlListener;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;

/**
 * Created by affo on 26/07/17.
 */
public abstract class ControlledSource<T> extends RichParallelSourceFunction<T> implements JobControlListener {
    protected transient Logger LOG;

    private Semaphore jobFinish = new Semaphore(0);
    protected transient JobControlClient jobControlClient;
    protected volatile boolean stop;
    protected int numberOfTasks, taskId;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(getClass().getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        jobControlClient.observe(this);

        numberOfTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        taskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    protected void waitForFinish() throws InterruptedException {
        jobFinish.acquire();
    }

    @Override
    public void cancel() {
        stop = true;
    }

    @Override
    public void onJobFinish() {
        JobControlListener.super.onJobFinish();
        stop = true;
        jobFinish.release();
    }

    @Override
    public void onBatchEnd() {
        // adapter
    }
}
