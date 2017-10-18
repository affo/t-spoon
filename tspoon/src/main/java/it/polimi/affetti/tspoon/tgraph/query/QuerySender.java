package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.ClientsCache;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ObjectClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Created by affo on 02/08/17.
 */
public class QuerySender extends RichSinkFunction<Query> {
    private transient Logger LOG;
    private transient JobControlClient jobControlClient;
    private transient ClientsCache<ObjectClient> queryServers;
    private Map<String, Set<Address>> addressesCache = new HashMap<>();
    private final OnQueryResult onQueryResult;

    private boolean verbose = true;

    public QuerySender() {
        onQueryResult = queryResult -> {
            if (verbose) {
                System.out.println(queryResult.toString());
            }
        };
    }

    public QuerySender(OnQueryResult onQueryResult) {
        this.onQueryResult = onQueryResult;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(QuerySender.class);

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
    @SuppressWarnings("unchecked")
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
        try {
            Map<String, Object> queryResult = new HashMap<>();
            for (Address address : addresses) {
                // could raise NPE
                ObjectClient queryServer = queryServers.getOrCreateClient(address);

                queryServer.send(query);
                Map<String, Object> partialResult = (Map<String, Object>) queryServer.receive();
                queryResult.putAll(partialResult);
            }

            onQueryResult.accept(queryResult);
        } catch (IOException e) {
            LOG.info("Query discarded because of IOException. Query: " + query
                    + ", Exception message: " + e.getMessage() + ".");
        }
    }

    public interface OnQueryResult extends Consumer<Map<String, Object>>, Serializable {
    }
}
