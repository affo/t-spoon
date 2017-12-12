package it.polimi.affetti.tspoon.runtime;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Created by affo on 08/12/17.
 */
public class WithJobControlClient implements JobControlListener {
    private transient Logger LOG;
    private transient JobControlClient jobControlClient;
    private Consumer<Void> onFinishCallback, onBatchEndCallback;

    public void open(RuntimeContext runtimeContext) throws IOException {
        LOG = Logger.getLogger(getClass().getSimpleName());

        ParameterTool parameterTool = (ParameterTool)
                runtimeContext.getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
    }

    public void observe() throws IOException {
        if (jobControlClient.isTunable()) {
            jobControlClient.observe(this);
        }
    }

    public void close() throws IOException {
        jobControlClient.close();
    }

    public void setOnFinishCallback(Consumer<Void> callback) {
        this.onFinishCallback = callback;
    }

    public void setOnBatchEndCallback(Consumer<Void> callback) {
        this.onBatchEndCallback = callback;
    }

    @Override
    public void onJobFinish() {
        if (onFinishCallback != null) {
            onFinishCallback.accept(null);
        }
    }

    @Override
    public void onBatchEnd() {
        // adapter
        if (onBatchEndCallback != null) {
            onBatchEndCallback.accept(null);
        }
    }

    public JobControlClient getJobControlClient() {
        return jobControlClient;
    }
}
