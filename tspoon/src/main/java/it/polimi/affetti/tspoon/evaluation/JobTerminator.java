package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 23/05/18.
 */
public class JobTerminator<T> extends RichSinkFunction<T> {
    private final TransientPeriod transientPeriod;
    private final TransientPeriod killer;
    private IntCounter generatedRecords = new IntCounter();
    private transient JobControlClient jobControlClient;

    public JobTerminator(TransientPeriod transientPeriod, int runtimeSeconds) {
        this.transientPeriod = transientPeriod;
        this.killer = new TransientPeriod(runtimeSeconds);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        getRuntimeContext().addAccumulator("collected-records", generatedRecords);

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        transientPeriod.start();

        transientPeriod.start(
                () -> killer.start(this::killJob)
        );
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    private void killJob() {
        jobControlClient.publishFinishMessage();
    }

    @Override
    public void invoke(T o) throws Exception {
        if (transientPeriod.hasFinished()) {
            generatedRecords.add(1);
        }
    }
}
