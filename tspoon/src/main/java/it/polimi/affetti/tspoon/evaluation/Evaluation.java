package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.PrintWriter;
import java.util.*;

public class Evaluation {
    public static final double startAmount = 100d;
    public static final String RECORD_TRACKING_SERVER_NAME = "request-tracker";

    public static long getWaitPeriodInMicroseconds(double frequency) {
        return (long) (Math.pow(10, 6) / frequency);
    }

    public static void main(String[] args) throws Exception {
        // ---------------------------- Params
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final String label = parameters.get("label");
        final String propertiesFile = parameters.get("propsFile", null);

        final int sourcePar = parameters.getInt("sourcePar", 1);
        final int par = parameters.getInt("par", 4) - sourcePar;
        final int partitioning = parameters.getInt("partitioning", 4) - sourcePar;
        final int keySpaceSize = parameters.getInt("ks", 100000);
        final int noTGraphs = parameters.getInt("noTG");
        final int noStates = parameters.getInt("noStates");
        final boolean seriesOrParallel = parameters.getBoolean("series");
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final boolean useDependencyTracking = parameters.getBoolean("dependencyTracking", true);
        final boolean synchronous = parameters.getBoolean("synchronous", false);
        final boolean durable = parameters.getBoolean("durable", false);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);
        final int openServerPoolSize = parameters.getInt("openPool", 1);
        final int stateServerPoolSize = parameters.getInt("statePool", 1);
        final int queryServerPoolSize = parameters.getInt("queryPool", 1);

        final boolean printPlan = parameters.getBoolean("printPlan", false);
        final boolean baselineMode = parameters.getBoolean("baseline", false);

        final int batchSize = parameters.getInt("batchSize", 100000);
        final int resolution = parameters.getInt("resolution", 100);
        final int maxNumberOfBatches = parameters.getInt("numberOfBatches", -1);

        assert par > 0;
        assert partitioning > 0;
        assert sourcePar > 0;
        assert noStates > 0;
        assert noTGraphs > 0;
        // if more than 1 tGraph, then we have only 1 state per tgraph
        assert !(noTGraphs > 1) || noStates == 1;
        assert isolationLevelNumber >= 0 && isolationLevelNumber <= 4;

        final Strategy strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.values()[isolationLevelNumber];

        // get startInputRate from props file -> this should enable us to run faster examples
        int startInputRate = parameters.getInt("startInputRate", -1);
        if (startInputRate < 0) {
            if (propertiesFile != null) {
                ParameterTool fromProps = ParameterTool.fromPropertiesFile(propertiesFile);
                String strStrategy = optimisticOrPessimistic ? "TB" : "LB"; // timestamp-based or lock-based
                String key = String.format("%s.%s.%s", label, strStrategy, isolationLevel.toString());
                startInputRate = fromProps.getInt(key, -1);
            }

            if (startInputRate < 0) {
                startInputRate = 1000;
            }
        }

        // merge the value obtained for startInputRate for later checking
        Map<String, String> toMerge = new HashMap<>();
        toMerge.put("startInputRate", String.valueOf(startInputRate));
        parameters = parameters.mergeWith(ParameterTool.fromMap(toMerge));

        System.out.println("\n>>>");
        System.out.println(parameters.toMap());
        System.out.println("<<<\n");

        // ---------------------------- Application

        final boolean local = (label == null || label.startsWith("local"));
        StreamExecutionEnvironment env;
        if (local) { // TODO the only way to make slot sharing group work locally for now...
            Configuration conf = new Configuration();
            conf.setInteger("taskmanager.numberOfTaskSlots", par + sourcePar);
            env = StreamExecutionEnvironment.createLocalEnvironment(par, conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(par);
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Flink suggests to keep it within 5 and 10 ms:
        // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html#controlling-latency
        // Anyway, we keep it to 0 to be as reactive as possible when new records are produced.
        // Buffering could lead to unexpected behavior (in terms of performance) in the transaction management.
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);

        // do not launch jobControlServer in case of printing the plan
        if (!printPlan) {
            NetUtils.launchJobControlServer(parameters);
        }

        env.getConfig().setGlobalJobParameters(parameters);

        // ---------------------------- Topology
        TransactionEnvironment.clear();
        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(strategy, isolationLevel);
        tEnv.setUseDependencyTracking(useDependencyTracking);
        tEnv.setSynchronous(synchronous);
        tEnv.setVerbose(false);
        tEnv.setOpenServerPoolSize(openServerPoolSize);
        tEnv.setStateServerPoolSize(stateServerPoolSize);
        tEnv.setQueryServerPoolSize(queryServerPoolSize);
        tEnv.setBaselineMode(baselineMode);
        final String sourceSharingGroupName = "sources";
        tEnv.setSourcesSharingGroup(sourceSharingGroupName, sourcePar);

        if (durable) {
            tEnv.enableDurability();
            NetUtils.launchWALServer(parameters);
        }

        // We assign source to a particular slot sharing group to avoid interference
        // in performance

        TunableSource.TunableTransferSource tunableSource =
                new TunableSource.TunableTransferSource(
                        startInputRate, resolution, batchSize, RECORD_TRACKING_SERVER_NAME);
        tunableSource.enableBusyWait();

        DataStream<Transfer> transfers = env.addSource(tunableSource)
                .name("TunableParallelSource").setParallelism(sourcePar)
                .slotSharingGroup(sourceSharingGroupName)
                .map(new TunableSource.ToTransfers(keySpaceSize, startAmount))
                .name("ToTransfers").setParallelism(sourcePar); // still in sources group

        // in case of parallel tgraphs, split the original stream for load balancing
        SplitStream<Transfer> splitTransfers = null;
        if (!seriesOrParallel) {
            splitTransfers = transfers.split(
                    t -> Collections.singletonList(String.valueOf(t.f0.f1 % noTGraphs)));
        }

        // ---------------------------- Composing
        EvaluationGraphComposer.startAmount = startAmount;
        EvaluationGraphComposer.setTransactionEnvironment(tEnv);

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

        EndToEndTracker endEndToEndTracker = new EndToEndTracker(false);

        SingleOutputStreamOperator<Transfer> afterEndTracking = out
                .process(endEndToEndTracker)
                .setParallelism(1)
                .name("EndTracker")
                .setBufferTimeout(0);

        DataStream<TransferID> endTracking = afterEndTracking
                .getSideOutput(endEndToEndTracker.getRecordTracking());

        // ---------------------------- Calculate Metrics
        endTracking // attach only to end tracking, we use a server for begin requests.
                .addSink(
                        new FinishOnBackPressure<>(0.25, batchSize, startInputRate, resolution,
                                maxNumberOfBatches, RECORD_TRACKING_SERVER_NAME))
                .setParallelism(1).name("FinishOnBackPressure");

        if (TransactionEnvironment.get(env).isVerbose()) {
            wal.print();
        }

        if (printPlan) {
            PrintWriter printWriter = new PrintWriter("plan.json");
            String executionPlan = env.getExecutionPlan();
            System.out.println(executionPlan);
            printWriter.print(executionPlan);
            return;
        }

        // ---------------------------- Executing
        JobExecutionResult jobExecutionResult = env.execute("Evaluation - " + label);

        // ---------------------------- Print report for local execution
        Report report = new Report("report");
        report.addAccumulators(jobExecutionResult);
        report.addField("parameters", parameters.toMap());
        report.writeToFile();
    }
}
