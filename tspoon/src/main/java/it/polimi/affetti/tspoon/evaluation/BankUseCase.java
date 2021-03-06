package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.SinglePartitionCommand;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.backed.TunableSPUSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.FrequencyQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.RandomQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.state.*;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class BankUseCase {
    public static final String SPU_TRACKING_SERVER_NAME = "spu-tracker";
    public static final double startAmount = 100;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);

        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final double queryFrequency = parameters.getDouble("queryRate", 100);
        final long inputWaitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);


        final int averageQuerySize = parameters.getInt("avg", 10);
        final int stdDevQuerySize = parameters.getInt("stddev", 5);

        final boolean consistencyCheck = parameters.getBoolean("check", false);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, config.keySpaceSize, startAmount);
        transferSource.setMicroSleep(inputWaitPeriodMicro);

        if (consistencyCheck) {
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            queryID -> new PredicateQuery<>(nameSpace, queryID, new PredicateQuery.SelectAll<>()),
                            0.1));
        } else {
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            new RandomQuerySupplier(nameSpace, 0, Transfer.KEY_PREFIX, config.keySpaceSize,
                                    averageQuerySize, stdDevQuerySize), queryFrequency));
            // Uncomment if you want query results
            // tEnv.setOnQueryResult(new QueryResultMerger.PrintQueryResult());
        }

        RandomSPUSupplier spuSupplier = new DepositsAndWithdrawalsGenerator(
                nameSpace, Transfer.KEY_PREFIX, config.keySpaceSize, startAmount
        );
        TunableSPUSource tunableSPUSource = new TunableSPUSource(config, SPU_TRACKING_SERVER_NAME, spuSupplier);

        SingleOutputStreamOperator<SinglePartitionUpdate> spuStream = env.addSource(tunableSPUSource)
                .name("TunableSPUSource");

        tEnv.enableSPUpdates(spuStream);

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);
        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new Balances(), config.partitioning);


        DataStream<TransactionResult> output = tEnv.close(balances.leftUnchanged);

        // every TunableSource requires a Tracker...
        balances.spuResults
                .map(tr -> ((SinglePartitionUpdate) tr.f2).id).returns(SinglePartitionUpdateID.class)
                .addSink(new Tracker<>(SPU_TRACKING_SERVER_NAME))
                .name("EndTracker")
                .setParallelism(1);

        if (consistencyCheck) {
            balances.queryResults.addSink(new ConsistencyCheck.CheckOnQueryResult(startAmount));
            FinishOnCountSink<TransactionResult> finishOnCountSink = new FinishOnCountSink<>(config.batchSize);
            finishOnCountSink.notSevere();
            output.addSink(finishOnCountSink)
                    .name("FinishOnCount")
                    .setParallelism(1);
        }

        String label = config.strategy + " - " + config.isolationLevel + ": ";

        if (consistencyCheck) {
            label += "Bank example consistency check";
        } else {
            label += "Bank example SPU evaluation";
        }

        env.execute(label);
    }

    private static class Balances implements StateFunction<Movement, Double> {
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
            return balance >= 0;
        }

        @Override
        public void apply(Movement element, ObjectHandler<Double> handler) {
            // this is the transaction:
            // r(x) w(x)
            handler.write(handler.read() + element.f2);
        }

        @SinglePartitionCommand
        public double deposit(double amount, double currentBalance) {
            return currentBalance + amount;
        }

        @SinglePartitionCommand
        public double withdrawal(double amount, double currentBalance) {
            return currentBalance - amount;
        }
    }
}
