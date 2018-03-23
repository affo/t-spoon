package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.SinglePartitionCommand;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.FrequencyQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.RandomQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.state.*;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class BankUseCase {
    public static final String SPU_TRACKING_SERVER_NAME = "spu-tracker";
    public static final double startAmount = 100;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final double queryFrequency = parameters.getDouble("queryRate", 100);
        final long inputWaitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 10000);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);

        final int averageQuerySize = parameters.getInt("avg", 10);
        final int stdDevQuerySize = parameters.getInt("stddev", 5);

        final int batchSize = parameters.getInt("batchSize", 10000);
        final int resolution = parameters.getInt("resolution", 100);
        final int startInputRate = parameters.getInt("startInputRate", 100);

        final boolean consistencyCheck = parameters.getBoolean("check", false);

        NetUtils.launchJobControlServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        final Strategy strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.values()[isolationLevelNumber];
        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(strategy, isolationLevel);
        tEnv.setSynchronous(false);
        tEnv.setDurable(false);
        tEnv.setStateServerPoolSize(Runtime.getRuntime().availableProcessors());

        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, keySpaceSize, startAmount);
        transferSource.setMicroSleep(inputWaitPeriodMicro);


        if (consistencyCheck) {
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            queryID -> new PredicateQuery<>(nameSpace, queryID, new PredicateQuery.SelectAll<>()),
                            0.1));
        } else {
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            new RandomQuerySupplier(nameSpace, 0, Transfer.KEY_PREFIX, keySpaceSize,
                                    averageQuerySize, stdDevQuerySize), queryFrequency));
            // Uncomment if you want query results
            // tEnv.setOnQueryResult(new QueryResultMerger.PrintQueryResult());
        }

        RandomSPUSupplier spuSupplier = new DepositsAndWithdrawalsGenerator(
                nameSpace, Transfer.KEY_PREFIX, keySpaceSize, startAmount
        );
        TunableSource.TunableSPUSource tunableSPUSource = new TunableSource.TunableSPUSource(
                startInputRate, resolution, batchSize, SPU_TRACKING_SERVER_NAME, spuSupplier
        );

        if (!consistencyCheck) {
            tunableSPUSource.enableBusyWait();
        }

        DataStream<SinglePartitionUpdate> spuStream = env.addSource(tunableSPUSource)
                .name("TunableSPUSource")
                // parallelism is set to 1 to have a single threaded busy wait
                .setParallelism(1);

        tEnv.enableSPUpdates(spuStream);

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);
        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new Balances(), partitioning);


        DataStream<TransactionResult> output = tEnv.close(balances.leftUnchanged);

        // every TunableSource requires a FinishOnBackPressure...
        balances.spuResults
                .map(tr -> ((SinglePartitionUpdate) tr.f2).id).returns(SinglePartitionUpdateID.class)
                .addSink(
                        new FinishOnBackPressure<>(
                                0.25, batchSize, startInputRate,
                                resolution, -1, SPU_TRACKING_SERVER_NAME))
                .name("FinishOnBackPressure")
                .setParallelism(1);

        if (consistencyCheck) {
            balances.queryResults.addSink(new ConsistencyCheck.CheckOnQueryResult(startAmount));
            FinishOnCountSink<TransactionResult> finishOnCountSink = new FinishOnCountSink<>(batchSize);
            finishOnCountSink.notSevere();
            output.addSink(finishOnCountSink)
                    .name("FinishOnCount")
                    .setParallelism(1);
        }

        String label = strategy + " - " + isolationLevel + ": ";

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
