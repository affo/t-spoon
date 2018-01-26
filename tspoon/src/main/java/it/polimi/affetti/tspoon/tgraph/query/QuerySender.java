package it.polimi.affetti.tspoon.tgraph.query;

import io.netty.util.concurrent.DefaultThreadFactory;
import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.ClientsCache;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.ObjectClient;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Created by affo on 02/08/17.
 */
public class QuerySender extends RichFlatMapFunction<Query, QueryResult> {
    private static transient Logger LOG;
    private transient JobControlClient jobControlClient;
    private transient ClientsCache<ObjectClient> queryServers;
    private Map<String, Set<Address>> addressesCache = new HashMap<>();
    private final OnQueryResult onQueryResult;
    private transient ExecutorService deferredExecutor;

    private static boolean verbose = false;

    public static final String QUERY_LATENCY_METRIC_NAME = "query-latency";

    private MetricAccumulator queryLatency = new MetricAccumulator();

    public QuerySender() {
        this(null);
    }

    public QuerySender(OnQueryResult onQueryResult) {
        if (onQueryResult == null) {
            this.onQueryResult = new NOPOnQueryResult();
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
        deferredExecutor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("QueryExecutor @ " + getRuntimeContext().getTaskNameWithSubtasks()));

        getRuntimeContext().addAccumulator(QUERY_LATENCY_METRIC_NAME, queryLatency);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jobControlClient != null) {
            jobControlClient.close();
        }

        queryServers.clear();
        deferredExecutor.shutdown();
    }

    @Override
    public void flatMap(Query query, Collector<QueryResult> collector) throws Exception {
        String nameSpace = query.getNameSpace();
        Set<Address> addresses = addressesCache.get(query.getNameSpace());
        try {
            if (addresses == null) {
                addresses = jobControlClient.discoverServers(nameSpace);
                addressesCache.put(nameSpace, addresses);
            }
        } catch (IOException e) {
            LOG.error("Problem in discovering query server for " + nameSpace + ": " + e.getMessage());
            return;
        }

        Set<Address> finalAddresses = addresses;
        Callable<Void> deferredExecution = () -> {
            try {
                long start = System.nanoTime();
                QueryResult queryResult = new QueryResult(query.getQueryID());
                for (Address address : finalAddresses) {
                    // could raise NPE
                    ObjectClient queryServer = queryServers.getOrCreateClient(address);

                    queryServer.send(query);
                    QueryResult partialResult = (QueryResult) queryServer.receive();
                    queryResult.merge(partialResult);
                }

                long end = System.nanoTime();
                long latency = (long) ((end - start) / Math.pow(10, 6));
                queryLatency.add((double) latency);

                collector.collect(queryResult);

                start = System.nanoTime();
                onQueryResult.accept(queryResult);
                end = System.nanoTime();
                long executionTime = (long) ((end - start) / Math.pow(10, 6));

                if (verbose) {
                    LOG.info("Query answered in " + latency + " ms, executed in " + executionTime + " ms");
                }
            } catch (IOException e) {
                LOG.info("Query discarded because of IOException. Query: " + query
                        + ", Exception message: " + e.getMessage() + ".");
            }

            return null;
        };

        deferredExecutor.submit(deferredExecution);
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

    public static class NOPOnQueryResult implements OnQueryResult {

        @Override
        public void accept(QueryResult queryResult) {
            // does nothing
        }
    }
}
