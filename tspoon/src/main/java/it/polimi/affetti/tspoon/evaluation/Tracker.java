package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.StringClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 06/06/18.
 */
public class Tracker<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
    private transient StringClient requestTrackerClient;
    private String trackingServerNameForDiscovery;

    public Tracker(String trackingServerNameForDiscovery) {
        this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        JobControlClient jobControlClient = JobControlClient.get(parameterTool);

        Address address = jobControlClient.discoverServer(trackingServerNameForDiscovery);
        jobControlClient.close();

        requestTrackerClient = new StringClient(address.ip, address.port);
        requestTrackerClient.init();
    }

    @Override
    public void close() throws Exception {
        super.close();
        requestTrackerClient.close();
    }

    @Override
    public void invoke(T t) throws Exception {
        requestTrackerClient.send(t.getUniqueRepresentation());
    }
}
