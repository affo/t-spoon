package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.TunableSource;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.query.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Evaluation {
    public static final double startAmount = 100d;

    private static long getWaitPeriodInMicroseconds(double frequency) {
        return (long) (Math.pow(10, 6) / frequency);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Flink suggests to keep it within 5 and 10 ms:
        // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html#controlling-latency
        // Anyway, we keep it to 0 to be as reactive as possible when new records are produced.
        // Buffering could lead to unexpected behavior (in terms of performance) in the transaction management.
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final String label = parameters.get("label");
        final int numRecords = parameters.getInt("nRec", 500000);
        final int sledLen = parameters.getInt("sled", 20000);
        final double inputFrequency = parameters.getDouble("inputRate", -1);
        final long waitPeriodMicro = getWaitPeriodInMicroseconds(inputFrequency);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 100000);
        final int noTGraphs = parameters.getInt("noTG");
        final int noStates = parameters.getInt("noStates");
        final boolean seriesOrParallel = parameters.getBoolean("series");
        final boolean queryOn = parameters.getBoolean("queryOn", false);
        final double queryPerc = parameters.getDouble("queryPerc", 0.0);
        final double queryRate = parameters.getDouble("queryRate", 0.0);
        final boolean transfersOn = parameters.getBoolean("transfersOn", true);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final boolean useDependencyTracking = parameters.getBoolean("dependencyTracking", true);
        final boolean synchronous = parameters.getBoolean("synchronous", true);
        final boolean durable = parameters.getBoolean("durable", true);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);
        final long deadlockTimeout = parameters.getLong("deadlockTimeout", 100L);
        final int openServerPoolSize = parameters.getInt("openPool", 1);
        final int stateServerPoolSize = parameters.getInt("statePool", 1);
        final int queryServerPoolSize = parameters.getInt("queryPool", 1);

        final boolean printPlan = parameters.getBoolean("printPlan", false);
        final boolean baselineMode = parameters.getBoolean("baseline", false);

        // parameters specific for tunable evaluation
        final boolean tunableExperiment = parameters.getBoolean("tunable", false);
        final int batchSize = parameters.getInt("batchSize", 50000);
        final int resolution = parameters.getInt("resolution", 200);
        final int startInputRate = parameters.getInt("startInputRate", 1000);

        assert noStates > 0;
        assert noTGraphs > 0;
        //assert 0 < throughputPerc && throughputPerc <= 1;
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

        // do not launch jobControlServer in case of printing the plan
        if (!printPlan) {
            NetUtils.launchJobControlServer(parameters);
        }

        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        // >>>>> Topology
        TransactionEnvironment.clear();
        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.setStrategy(strategy);
        tEnv.setIsolationLevel(isolationLevel);
        tEnv.setUseDependencyTracking(useDependencyTracking);
        tEnv.setSynchronous(synchronous);
        tEnv.setDurable(durable);
        tEnv.setDeadlockTimeout(deadlockTimeout);
        tEnv.setVerbose(false);
        tEnv.setOpenServerPoolSize(openServerPoolSize);
        tEnv.setStateServerPoolSize(stateServerPoolSize);
        tEnv.setQueryServerPoolSize(queryServerPoolSize);
        tEnv.setBaselineMode(baselineMode);

        // >>> Source
        int limit = numRecords + sledLen;
        if (!transfersOn) {
            limit = 0;
        }

        DataStream<Transfer> transfers;
        if (tunableExperiment) {
            TunableSource.TunableTransferSource tunableSource =
                    new TunableSource.TunableTransferSource(startInputRate, resolution, batchSize);
            transfers = env.addSource(tunableSource)
                    .name("TunableParallelSource")
                    .map(new TunableSource.ToTransfers(keySpaceSize, startAmount))
                    .name("ToTransfers");
        } else {
            TransferSource transferSource = new TransferSource(limit, keySpaceSize, startAmount);
            if (waitPeriodMicro > 0) {
                transferSource.setMicroSleep(waitPeriodMicro);
            }
            transfers = env.addSource(transferSource).setParallelism(1);
        }

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

        DataStream<Transfer> afterSource = transfers;
        if (!tunableExperiment) {
            afterSource = transfers
                    .filter(new SkipFirstN<>(sledLen))
                    .setParallelism(1)
                    .name("SkipFirst" + sledLen);
        }

        // put a latency tracker at the beginning after the sled
        EndToEndTracker beginEndToEndTracker = new EndToEndTracker(true);
        // This tracks the generation of records, and does it by using a hand-managed channel to avoid back-pressure
        SingleOutputStreamOperator<Transfer> afterStartTracking = afterSource
                .process(beginEndToEndTracker)
                .name("BeginTracker");

        DataStream<TransferID> startTracking = afterStartTracking
                .getSideOutput(beginEndToEndTracker.getRecordTracking());

        // in case of parallel tgraphs, split the original stream for load balancing
        SplitStream<Transfer> splitTransfers = null;
        if (!seriesOrParallel) {
            splitTransfers = transfers.split(
                    t -> Collections.singletonList(String.valueOf(t.f0.f1 % noTGraphs)));
        }

        // >>> Composing
        EvaluationGraphComposer.startAmount = startAmount;
        EvaluationGraphComposer.setTransactionEnvironment(tEnv);
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
        DataStream<Transfer> afterOut = out;
        if (!tunableExperiment) {
            afterOut = out
                    .filter(new SkipFirstN<>(sledLen))
                    .setParallelism(1)
                    .name("SkipFirst" + sledLen);
        }

        EndToEndTracker endEndToEndTracker = new EndToEndTracker(false);

        SingleOutputStreamOperator<Transfer> afterEndTracking = afterOut
                .process(endEndToEndTracker)
                .setParallelism(1)
                .name("EndTracker")
                .setBufferTimeout(0);

        DataStream<TransferID> endTracking = afterEndTracking
                .getSideOutput(endEndToEndTracker.getRecordTracking());

        TypeInformation<Tuple2<TransferID, Boolean>> tupleTypeInfo =
                TypeInformation.of(new TypeHint<Tuple2<TransferID, Boolean>>() {
                });
        DataStream<Tuple2<TransferID, Boolean>> trackingStream = startTracking
                .map(tid -> Tuple2.of(tid, true))
                .returns(tupleTypeInfo)
                .name("AddBeginBoolean")
                .union(
                        endTracking
                                .map(tid -> Tuple2.of(tid, false))
                                .returns(tupleTypeInfo)
                                .name("AddEndBoolean")
                );

        if (tunableExperiment) {
            // >>> Add FinishOnBackPressure
            endTracking // attach only to end tracking. We will use a server for begin requests.
                    .addSink(new FinishOnBackPressure(0.25, batchSize, startInputRate, resolution))
                    .setParallelism(1).name("FinishOnBackPressure");
        } else {
            // >>> Add latency calculator
            trackingStream
                    .flatMap(new TimestampDeltaFunction())
                    .setParallelism(1).name("TimestampDelta");

            // >>> Add ThroughputCalculator
            afterOut // calculate throughput after the sled.
                    // in this case, the batchSize is the subBatch size...
                    // the batchSize is the total number of records
                    .map(new ThroughputCalculator<>(batchSize))
                    .setParallelism(1).name("ThroughputCalculator");

            // >>> Add FinishOnCount
            wal
                    .filter(entry -> entry.f1 != Vote.REPLAY)
                    .addSink(new FinishOnCountSink<>(numberOfElements))
                    .setParallelism(1).name("FinishOnCount");
        }

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

        // >>>>> Executing
        JobExecutionResult jobExecutionResult = env.execute("Evaluation - " + label);

        // >>> Only for local execution
        Report report = new Report("report");
        report.addAccumulators(jobExecutionResult);
        report.addField("parameters", parameters.toMap());
        report.writeToFile();
    }
}
