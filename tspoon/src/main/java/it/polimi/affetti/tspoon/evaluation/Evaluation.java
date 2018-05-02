package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Evaluation {
    public static final String RECORD_TRACKING_SERVER_NAME = "request-tracker";

    public static long getWaitPeriodInMicroseconds(double frequency) {
        return (long) (Math.pow(10, 6) / frequency);
    }

    public static void main(String[] args) throws Exception {
        // ---------------------------- Params
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);

        System.out.println("\n>>>");
        System.out.println(config.params.toMap());
        System.out.println("<<<\n");

        // ---------------------------- Application
        // do not launch jobControlServer in case of printing the plan
        if (!config.printPlan) {
            NetUtils.launchJobControlServer(parameters);
            if (config.durable) {
                NetUtils.launchWALServer(parameters);
            }
        }

        StreamExecutionEnvironment env = EvalUtils.getFlinkEnv(config);

        // ---------------------------- Topology
        TransactionEnvironment.clear();
        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(config.strategy, config.isolationLevel);
        tEnv.setUseDependencyTracking(config.useDependencyTracking);
        tEnv.setSynchronous(config.synchronous);
        tEnv.setVerbose(false);
        tEnv.setOpenServerPoolSize(config.openServerPoolSize);
        tEnv.setStateServerPoolSize(config.stateServerPoolSize);
        tEnv.setQueryServerPoolSize(config.queryServerPoolSize);
        tEnv.setBaselineMode(config.baselineMode);
        EvalUtils.setSourcesSharingGroup(tEnv);

        if (config.durable) {
            tEnv.enableDurability();
        }

        TunableSource.TunableTransferSource tunableSource =
                new TunableSource.TunableTransferSource(
                        config.startInputRate, config.resolution, config.batchSize, RECORD_TRACKING_SERVER_NAME);
        tunableSource.enableBusyWait();

        DataStreamSource<TransferID> dsSource = env.addSource(tunableSource);
        SingleOutputStreamOperator<TransferID> tidSource =
                EvalUtils.addToSourcesSharingGroup(dsSource, "TunableParallelSource");
        SingleOutputStreamOperator<Transfer> toTranfers = tidSource
                .map(new TunableSource.ToTransfers(config.keySpaceSize, EvalUtils.startAmount));
        DataStream<Transfer> transfers = EvalUtils.addToSourcesSharingGroup(toTranfers, "ToTransfers");

        // in case of parallel tgraphs, split the original stream for load balancing
        SplitStream<Transfer> splitTransfers = null;
        if (!config.seriesOrParallel) {
            splitTransfers = transfers.split(
                    t -> Collections.singletonList(String.valueOf(t.f0.f1 % config.noTGraphs)));
        }

        // ---------------------------- Composing
        EvaluationGraphComposer.setTransactionEnvironment(tEnv);

        List<DataStream<Transfer>> outs = new ArrayList<>(config.noTGraphs);
        int i = 0;
        do {
            if (!config.seriesOrParallel) {
                // select the portion of the source stream relevant to this state
                transfers = splitTransfers.select(String.valueOf(i));
            }
            DataStream<Transfer> out = EvaluationGraphComposer
                    .generateTGraph(transfers, config.noStates, config.partitioning, config.seriesOrParallel);
            outs.add(out);
            if (config.seriesOrParallel) {
                transfers = out;
            }
            i++;
        } while (i < config.noTGraphs);

        DataStream<Transfer> out;
        if (config.seriesOrParallel) {
            out = outs.get(config.noTGraphs - 1);
        } else {
            out = outs.get(0);
            for (DataStream<Transfer> o : outs.subList(1, config.noTGraphs)) {
                out = out.union(o);
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
                        new FinishOnBackPressure<>(
                                0.1, config.batchSize, config.startInputRate, config.resolution,
                                config.maxNumberOfBatches, RECORD_TRACKING_SERVER_NAME))
                .setParallelism(1).name("FinishOnBackPressure");

        if (TransactionEnvironment.get(env).isVerbose()) {
            out.print();
        }

        if (config.printPlan) {
            PrintWriter printWriter = new PrintWriter("plan.json");
            String executionPlan = env.getExecutionPlan();
            System.out.println(executionPlan);
            printWriter.print(executionPlan);
            return;
        }

        // ---------------------------- Executing
        JobExecutionResult jobExecutionResult = env.execute("Evaluation - " + config.label);

        // ---------------------------- Print report for local execution
        Report report = new Report("report");
        report.addAccumulators(jobExecutionResult);
        report.addField("parameters", parameters.toMap());
        report.writeToFile();
    }
}
