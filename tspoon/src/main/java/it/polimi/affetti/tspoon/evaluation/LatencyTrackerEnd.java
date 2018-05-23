package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.TimeDelta;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 23/05/18.
 */
public class LatencyTrackerEnd<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
    private transient WithServer requestTracker;
    private transient JobControlClient jobControlClient;
    private final String trackingServerName;
    private final String accumulatorName;
    private TimeDelta currentLatency;
    private boolean firstStart = false;

    public LatencyTrackerEnd(String trackingServerName, String accumulatorName) {
        this.trackingServerName = trackingServerName;
        this.accumulatorName = accumulatorName;
        this.currentLatency = new TimeDelta();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);

        requestTracker = new WithServer(new ProcessRequestServer() {
            @Override
            protected void parseRequest(String recordID) {
                start(recordID);
            }
        });
        requestTracker.open();
        jobControlClient.registerServer(trackingServerName, requestTracker.getMyAddress());

        getRuntimeContext().addAccumulator(accumulatorName, currentLatency.getNewAccumulator());
    }

    @Override
    public void close() throws Exception {
        super.close();
        requestTracker.close();
        jobControlClient.close();
    }

    @Override
    public synchronized void invoke(T t) throws Exception {
        if (firstStart) {
            currentLatency.end(t.getUniqueRepresentation());
        }
    }

    private synchronized void start(String recordID) {
        currentLatency.start(recordID);
        firstStart = true;
    }
}