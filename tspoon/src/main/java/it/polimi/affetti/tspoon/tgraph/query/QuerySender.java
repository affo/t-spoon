package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.TaskExecutor;
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
import java.util.function.Consumer;

/**
 * Created by affo on 02/08/17.
 */
public class QuerySender extends RichFlatMapFunction<Query, QueryResult>
        implements TaskExecutor.TaskErrorListener {
    private static transient Logger LOG;
    private transient JobControlClient jobControlClient;
    private transient ClientsCache<ObjectClient> queryServers;
    private Map<String, Set<Address>> addressesCache = new HashMap<>();
    private final OnQueryResult onQueryResult;
    private transient TaskExecutor deferredExecutor;
    private volatile Throwable error;

    private static boolean verbose = false;

    public static final String QUERY_LATENCY_METRIC_NAME = "query-latency-at-query-sender";
    public static final String QUERY_RESULT_SIZE_METRIC_NAME = "query-result-size";

    private MetricAccumulator queryLatency = new MetricAccumulator();
    private MetricAccumulator queryResultSize = new MetricAccumulator();

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
        deferredExecutor = new TaskExecutor();
        deferredExecutor.setName("QueryExecutor @ " + getRuntimeContext().getTaskNameWithSubtasks());
        deferredExecutor.start();

        getRuntimeContext().addAccumulator(QUERY_LATENCY_METRIC_NAME, queryLatency);
        getRuntimeContext().addAccumulator(QUERY_RESULT_SIZE_METRIC_NAME, queryResultSize);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jobControlClient != null) {
            jobControlClient.close();
        }

        queryServers.clear();
        deferredExecutor.interrupt();
    }

    @Override
    public void flatMap(Query query, Collector<QueryResult> collector) throws Exception {
        if (error != null) {
            // TODO find a better way
            throw new RuntimeException(error);
        }

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
        Runnable deferredExecution = () -> {
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
                queryResultSize.add((double) queryResult.getSize());

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
            } catch (ClassNotFoundException e) {
                LOG.info("Cannot cast message to QueryResult. Query: " + query
                        + ", Exception message: " + e.getMessage() + ".");
            }
        };

        deferredExecutor.addTask(deferredExecution, this);
    }

    @Override
    public void onTaskError(Throwable t) {
        error = t;
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
