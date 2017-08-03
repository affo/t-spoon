package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.ClientsCache;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ObjectClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by affo on 02/08/17.
 */
public class QuerySender extends RichSinkFunction<Query> {
    private transient JobControlClient jobControlClient;
    private transient ClientsCache<ObjectClient> queryServers;
    private Map<String, Set<Address>> addressesCache = new HashMap<>();

    private boolean verbose = true;

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        queryServers = new ClientsCache<>(address -> new ObjectClient(address.ip, address.port));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jobControlClient != null) {
            jobControlClient.close();
        }
        queryServers.clear();
    }

    @Override
    public void invoke(Query query) throws Exception {
        Set<Address> addresses = addressesCache.computeIfAbsent(query.getNameSpace(),
                nameSpace -> {
                    if (jobControlClient == null) {
                        return null;
                    }

                    try {
                        return jobControlClient.discoverQueryServer(nameSpace);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                });

        // TODO send back to somewhere
        StringBuilder log = new StringBuilder(">>> BEGIN QUERY: " + query + "\n");
        for (Address address : addresses) {
            // could raise NPE
            ObjectClient queryServer = queryServers.getOrCreateClient(address);

            queryServer.send(query);
            Object result = queryServer.receive();
            log.append("\t").append(result).append("\n");
        }
        log.append("<<< END QUERY");
        if (verbose) {
            System.out.println(log);
        }
    }
}
