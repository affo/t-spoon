package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.backed.TunableTransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class IncreaseAborts {
    public static final String RECORD_TRACKING_SERVER_NAME = "request-tracker";
    public static final double startAmount = 100;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);

        final int abortsPercentage = parameters.getInt("abortsPercentage", 0);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        TunableTransferSource tunableSource =
                new TunableTransferSource(config, RECORD_TRACKING_SERVER_NAME);

        DataStreamSource<TransferID> dsSource = env.addSource(tunableSource);
        SingleOutputStreamOperator<TransferID> tidSource =
                config.addToSourcesSharingGroup(dsSource, "TunableSource");
        SingleOutputStreamOperator<Transfer> toTranfers = tidSource
                .map(new TunableTransferSource.ToTransfers(config.keySpaceSize, EvalConfig.startAmount));
        DataStream<Transfer> transfers = config.addToSourcesSharingGroup(toTranfers, "ToTransfers");
        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new Balances(abortsPercentage), config.partitioning);


        DataStream<Transfer> out = tEnv
                .close(balances.leftUnchanged)
                .map(tr -> (Transfer) tr.f2)
                .returns(Transfer.class);
        EndToEndTracker endEndToEndTracker = new EndToEndTracker(false);

        SingleOutputStreamOperator<Transfer> afterEndTracking = out
                .process(endEndToEndTracker)
                .setParallelism(1)
                .name("EndTracker")
                .setBufferTimeout(0);

        DataStream<TransferID> endTracking = afterEndTracking
                .getSideOutput(endEndToEndTracker.getRecordTracking());

        endTracking
                .addSink(new Tracker<>(RECORD_TRACKING_SERVER_NAME))
                .setParallelism(1).name("EndTracker");

        JobExecutionResult results = env.execute("Bank example - random aborts");
        Report report = new Report("report");
        report.addAccumulators(results);
        report.addField("parameters", parameters.toMap());
        System.out.println(">>> BEGIN report");
        System.out.println(report.format());
        System.out.println("<<< END report");

    }

    private static class Balances implements StateFunction<Movement, Double> {
        private int abortProb;

        public Balances(int abortProb) {
            this.abortProb = abortProb;
        }

        private boolean withProb(int p) {
            return (Math.random() * 100) < p;
        }

        @Override
        public Double defaultValue() {
            return startAmount;
        }

        @Override
        public Double copyValue(Double balance) {
            return balance;
        }

        @Override
        public boolean invariant(Double balance) {
            return !withProb(abortProb);
        }

        @Override
        public void apply(Movement element, ObjectHandler<Double> handler) {
            // this is the transaction:
            // r(x) w(x)
            handler.write(handler.read() + element.f2);
        }
    }
}
