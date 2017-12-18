package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.Report;
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
    private static transient Logger LOG;
    private transient JobControlClient jobControlClient;
    private transient ClientsCache<ObjectClient> queryServers;
    private Map<String, Set<Address>> addressesCache = new HashMap<>();
    private final OnQueryResult onQueryResult;

    private static boolean verbose = true;

    public static final String QUERY_LATENCY_METRIC_NAME = "query-latency";

    private MetricAccumulator queryLatency = new MetricAccumulator();

    public QuerySender() {
        this(null);
    }

    public QuerySender(OnQueryResult onQueryResult) {
        if (onQueryResult == null) {
            this.onQueryResult = new LogQueryResult();
        } else {
            this.onQueryResult = onQueryResult;
        }

        Report.registerAccumulator(QUERY_LATENCY_METRIC_NAME);
    }

    public static void setVerbose(boolean verbose) {
        QuerySender.verbose = verbose;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG = Logger.getLogger(QuerySender.class);

        ParameterTool parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
        queryServers = new ClientsCache<>(address -> new ObjectClient(address.ip, address.port));

        getRuntimeContext().addAccumulator(QUERY_LATENCY_METRIC_NAME, queryLatency);
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

        long start = System.currentTimeMillis();
        try {
            QueryResult queryResult = new QueryResult();
            for (Address address : addresses) {
                // could raise NPE
                ObjectClient queryServer = queryServers.getOrCreateClient(address);

                queryServer.send(query);
                QueryResult partialResult = (QueryResult) queryServer.receive();
                queryResult.merge(partialResult);
            }

            onQueryResult.accept(queryResult);
        } catch (IOException e) {
            LOG.info("Query discarded because of IOException. Query: " + query
                    + ", Exception message: " + e.getMessage() + ".");
        }
        long end = System.currentTimeMillis();
        long latency = end - start;
        queryLatency.add((double) latency);

        if (verbose) {
            LOG.info("Query executed in " + latency + " ms");
        }
    }

    public interface OnQueryResult extends Consumer<QueryResult>, Serializable {
    }

    public static class PrintQueryResult implements OnQueryResult {

        @Override
        public void accept(QueryResult queryResult) {
            if (verbose) {
                System.out.println(queryResult.toString());
            }
        }
    }

    public static class LogQueryResult implements OnQueryResult {

        @Override
        public void accept(QueryResult queryResult) {
            LOG.info(queryResult.toString());
        }
    }
}
