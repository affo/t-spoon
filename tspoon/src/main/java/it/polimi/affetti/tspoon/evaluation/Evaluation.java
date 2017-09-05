package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.JobControlServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.runtime.TimestampDeltaServer;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.query.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Evaluation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setBufferTimeout(0);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final int numRecords = parameters.getInt("nRec", 500000);
        final int sledLen = parameters.getInt("sled", 20000);
        final double inputFrequency = parameters.getDouble("inputRate", -1);
        final long waitPeriodMicro = (long) ((1.0 / inputFrequency) * 1000000);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 100000);
        final String outputFile = parameters.getRequired("output");
        final int noTGraphs = parameters.getInt("noTG");
        final int noStates = parameters.getInt("noStates");
        final boolean seriesOrParallel = parameters.getBoolean("series");
        final boolean queryOn = parameters.getBoolean("queryOn", false);
        final double queryPerc = parameters.getDouble("queryPerc", 0.0);
        final double queryRate = parameters.getDouble("queryRate", 0.0);
        final boolean transfersOn = parameters.getBoolean("transfersOn", true);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final boolean useDependencyTracking = parameters.getBoolean("dependencyTracking", true);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);

        assert noStates > 0;
        assert noTGraphs > 0;
        // if more than 1 tGraph, then we have only 1 state per tgraph
        assert !(noTGraphs > 1) || noStates == 1;
        assert queryOn || transfersOn;
        // if queryRate > 0 then queryOn
        assert queryRate == 0.0 || queryOn;
        // if transfer is off input rate cannot be greater than zero
        // !trasferOn ==> inputFrequency <= 0
        assert transfersOn || inputFrequency <= 0;
        assert isolationLevelNumber >= 0 && isolationLevelNumber <= 4;

        final int batchSize = 10000;
        final double startAmount = 100d;
        final Strategy strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.values()[isolationLevelNumber];

        System.out.println("\n>>>");
        System.out.println(parameters.toMap());
        System.out.println("<<<\n");

        JobControlServer jobControlServer = NetUtils.launchJobControlServer(parameters);
        TimestampDeltaServer timestampDeltaServer = NetUtils.launchTimestampDeltaServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        // >>>>> Topology
        TransactionEnvironment tEnv = TransactionEnvironment.get();
        tEnv.setStrategy(strategy);
        tEnv.setIsolationLevel(isolationLevel);
        tEnv.setUseDependencyTracking(useDependencyTracking);

        // >>> Source
        int limit = numRecords + sledLen;
        if (!transfersOn) {
            limit = 0;
        }
        TransferSource transferSource = new TransferSource(limit, keySpaceSize, startAmount);
        if (waitPeriodMicro > 0) {
            transferSource.setMicroSleep(waitPeriodMicro);
        }

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);

        // >>> Querying
        // - if query is on and transfer is off, then we have to use nRec as queryCount
        // otherwise (query on and transfer on), we user nRec for transfers and in the meantime we query.
        // - queryPerc is used to query a percentage of the keyspace
        // - the keyspace set is derived from the source and -ks
        if (queryOn) {
            // TODO this part is still a draft. Decide on how to perform queries!
            assert 0 < queryPerc && queryPerc <= 1;
            int noKeys = (int) (keySpaceSize * noStates * queryPerc);

            QuerySupplier querySupplier = new PredefinedQuerySupplier(new RandomQuery("lol", noKeys));

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
        if (!seriesOrParallel) {
            transfers = transfers.split(
                    t -> Collections.singletonList(String.valueOf(t.f0 % noTGraphs)));
        }

        // >>> Composing
        EvaluationGraphComposer.startAmount = startAmount;
        EvaluationGraphComposer.numberOfElements = numRecords;

        List<DataStream<Transfer>> outputs = new ArrayList<>(noTGraphs);
        int i = 0;
        do {
            DataStream<Transfer> out = EvaluationGraphComposer
                    .generateTGraph(transfers, noStates, partitioning, seriesOrParallel);
            outputs.add(out);
            if (seriesOrParallel) {
                transfers = out;
            }
            i++;
        } while (i < noTGraphs);

        DataStream<Transfer> out;
        if (seriesOrParallel) {
            out = outputs.get(noTGraphs - 1);
        } else {
            out = outputs.get(0);
            for (DataStream<Transfer> o : outputs.subList(1, noTGraphs)) {
                out = out.union(o);
            }
        }

        // >>> Closing
        out.filter(new SkipFirstN<>(sledLen)).setParallelism(1)
                .map(new LatencyTracker(false)).setParallelism(1) // end tracker
                .map(new ElapsedTimeCalculator<>(batchSize)).setParallelism(1);

        // >>>>> Gathering Results
        JobExecutionResult result = env.execute();
        // close servers
        jobControlServer.close();
        timestampDeltaServer.close();

        Report report = new Report(outputFile);
        report.addAccumulators(result);
        report.addField("parameters", parameters.toMap());
        report.addFields(timestampDeltaServer.getMetrics());
        report.writeToFile();
    }
}
