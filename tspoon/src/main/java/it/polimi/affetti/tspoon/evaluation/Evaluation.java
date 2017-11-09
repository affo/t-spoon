package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.metrics.Metric;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.JobControlServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.runtime.TimestampDeltaServer;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.query.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class Evaluation {
    public static final int batchSize = 10000;
    public static final double startAmount = 100d;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setBufferTimeout(1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final int numRecords = parameters.getInt("nRec", 500000);
        final int sledLen = parameters.getInt("sled", 20000);
        final double inputFrequency = parameters.getDouble("inputRate", -1);
        final double throughputPerc = parameters.getDouble("throughPerc", 0.8);
        final long waitPeriodMicro = getWaitPeriodInMicroseconds(inputFrequency);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 100000);
        final String outputWithoutExtension = parameters.getRequired("output");
        final int noTGraphs = parameters.getInt("noTG");
        final int noStates = parameters.getInt("noStates");
        final boolean seriesOrParallel = parameters.getBoolean("series");
        final boolean queryOn = parameters.getBoolean("queryOn", false);
        final double queryPerc = parameters.getDouble("queryPerc", 0.0);
        final double queryRate = parameters.getDouble("queryRate", 0.0);
        final boolean transfersOn = parameters.getBoolean("transfersOn", true);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final boolean useDependencyTracking = parameters.getBoolean("dependencyTracking", true);
        // TODO implement switchable durability
        final boolean durable = parameters.getBoolean("durable", true);
        final boolean noLatency = parameters.getBoolean("noLatency", false);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);

        final boolean printPlan = parameters.getBoolean("printPlan", false);


        assert noStates > 0;
        assert noTGraphs > 0;
        assert 0 < throughputPerc && throughputPerc <= 1;
        // if more than 1 tGraph, then we have only 1 state per tgraph
        assert !(noTGraphs > 1) || noStates == 1;
        assert queryOn || transfersOn;
        // if queryRate > 0 then queryOn
        assert queryRate == 0.0 || queryOn;
        // if transfer is off input rate cannot be greater than zero
        // !trasferOn ==> inputFrequency <= 0
        assert transfersOn || inputFrequency <= 0;
        assert isolationLevelNumber >= 0 && isolationLevelNumber <= 4;

        final Strategy strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.values()[isolationLevelNumber];

        System.out.println("\n>>>");
        System.out.println(parameters.toMap());
        System.out.println("<<<\n");

        JobControlServer jobControlServer = null;
        TimestampDeltaServer timestampDeltaServer = null;

        // do not launch servers in case of printing the plan
        if (!printPlan) {
            jobControlServer = NetUtils.launchJobControlServer(parameters);
            timestampDeltaServer = NetUtils.launchTimestampDeltaServer(parameters);
        }

        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        // creates a consumer that draws an evaluation topology based on the value
        // of the waiting period. If the microSleep is negative, the consumer will be re-invoked with the
        // value of the input rate calculated with the throughput of the first job (see later).
        Consumer<Long> drawEvaluationTopology = (microSleep) -> {
            // >>>>> Topology
            TransactionEnvironment.clear();
            TransactionEnvironment tEnv = TransactionEnvironment.get();
            tEnv.setStrategy(strategy);
            tEnv.setIsolationLevel(isolationLevel);
            tEnv.setUseDependencyTracking(useDependencyTracking);
            tEnv.setVerbose(false);

            // >>> Source
            int limit = numRecords + sledLen;
            if (!transfersOn) {
                limit = 0;
            }
            TransferSource transferSource = new TransferSource(limit, keySpaceSize, startAmount);
            if (microSleep > 0) {
                transferSource.setMicroSleep(microSleep);
            }

            DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);

            // >>> Querying
            // NOTE: this part is only relevant in the case of the simplest topology (1 TG, 1 state).
            // - if query is on and transfer is off, then we have only queries and no transfers =>
            //      we use nRec as the number of queries to perform;
            // - otherwise (query on and transfer on), we have a mixed load of queries and updates and
            //      the purpose is to run queries at a certain rate while updating the state =>
            //      we user nRec for transfers and queryRate for the rate of querying.
            // NOTE: the query issued is a GET on a percentage (queryPerc) of the keyspace managed by the unique state.
            if (queryOn) {
                assert 0 < queryPerc && queryPerc <= 1;
                assert noStates == 1 && noTGraphs == 1; // the topology should be the simplest one
                int noKeys = (int) (keySpaceSize * queryPerc);

                QuerySupplier querySupplier = new PredefinedQuerySupplier(
                        new RandomQuery(EvaluationGraphComposer.STATE_BASE_NAME + "0", noKeys));

                if (!transfersOn) {
                    querySupplier = new LimitQuerySupplier(querySupplier, numRecords);
                }

                if (queryRate > 0) {
                    querySupplier = new FrequencyQuerySupplier(querySupplier, queryRate);
                }

                tEnv.setQuerySupplier(querySupplier);
            }

            // put a latency tracker at the beginning after the sled
            transfers.filter(new SkipFirstN<>(sledLen)).setParallelism(1)
                    .map(new LatencyTracker(true)).setParallelism(1);
            // in case of parallel tgraphs, split the original stream for load balancing
            SplitStream<Transfer> splitTransfers = null;
            if (!seriesOrParallel) {
                splitTransfers = transfers.split(
                        t -> Collections.singletonList(String.valueOf(t.f0 % noTGraphs)));
            }

            // >>> Composing
            EvaluationGraphComposer.startAmount = startAmount;
            int numberOfElements = numRecords + sledLen;

            List<EvaluationGraphComposer.TGraph> tGraphs = new ArrayList<>(noTGraphs);
            int i = 0;
            do {
                if (!seriesOrParallel) {
                    // select the portion of the source stream relevant to this state
                    transfers = splitTransfers.select(String.valueOf(i));
                }
                EvaluationGraphComposer.TGraph tGraph = EvaluationGraphComposer
                        .generateTGraph(transfers, noStates, partitioning, seriesOrParallel);
                tGraphs.add(tGraph);
                DataStream<Transfer> out = tGraph.getOut();
                if (seriesOrParallel) {
                    transfers = out;
                }
                i++;
            } while (i < noTGraphs);

            DataStream<Transfer> out;
            DataStream<Tuple2<Long, Vote>> wal;
            if (seriesOrParallel) {
                EvaluationGraphComposer.TGraph lastTGraph = tGraphs.get(noTGraphs - 1);
                out = lastTGraph.getOut();
                wal = lastTGraph.getWal();
            } else {
                EvaluationGraphComposer.TGraph firstTGraph = tGraphs.get(0);
                out = firstTGraph.getOut();
                wal = firstTGraph.getWal();
                for (EvaluationGraphComposer.TGraph tg : tGraphs.subList(1, noTGraphs)) {
                    out = out.union(tg.getOut());
                    wal = wal.union(tg.getWal());
                }
            }

            // >>> Closing
            out.filter(new SkipFirstN<>(sledLen)).setParallelism(1)
                    .map(new LatencyTracker(false)).setParallelism(1) // end tracker
                    .map(new ThroughputCalculator<>(batchSize)).setParallelism(1);

            // >>> Add FinishOnCount
            wal
                    .filter(entry -> entry.f1 != Vote.REPLAY)
                    .addSink(new FinishOnCountSink<>(numberOfElements))
                    .setParallelism(1).name("FinishOnCount");

            if (TransactionEnvironment.get().isVerbose()) {
                wal.print();
            }
        };

        drawEvaluationTopology.accept(waitPeriodMicro);

        if (printPlan) {
            PrintWriter printWriter = new PrintWriter("plan.json");
            String executionPlan = env.getExecutionPlan();
            System.out.println(executionPlan);
            printWriter.print(executionPlan);
            return;
        }

        // >>>>> Gathering Results
        JobExecutionResult result = env.execute("Evaluation - " + outputWithoutExtension);

        Report report = new Report(outputWithoutExtension);
        report.addAccumulators(result);
        report.addField("parameters", parameters.toMap());

        // This means you have to launch a latency test,
        // because the first one hadn't been made for that purpose!
        if (waitPeriodMicro < 0 && !noLatency) {
            // >>>>> Launching a latency job
            Metric throughputMetric = result.getAccumulatorResult(ThroughputCalculator.THROUGHPUT_ACC);
            double meanThroughput = throughputMetric.toMap().get("mean");
            double inputRate = Math.floor(meanThroughput * throughputPerc);

            report.updateField("parameters", "inputRate", inputRate);
            System.out.println("\n\n>>> Executing latency experiment with input rate = "
                    + meanThroughput + " * " + throughputPerc + " = " + inputRate);
            Thread.sleep(4000);

            timestampDeltaServer.clearMetrics();

            drawEvaluationTopology.accept(getWaitPeriodInMicroseconds(inputRate));

            env.execute("Latency Evaluation - " + outputWithoutExtension);
        }

        // close servers
        jobControlServer.close();
        timestampDeltaServer.close();

        report.addFields(timestampDeltaServer.getMetrics());
        report.writeToFile();
    }

    private static long getWaitPeriodInMicroseconds(double inputFrequency) {
        return (long) ((1.0 / inputFrequency) * 1000000);
    }
}
