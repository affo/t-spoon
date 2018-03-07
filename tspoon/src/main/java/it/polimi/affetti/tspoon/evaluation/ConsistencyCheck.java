package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.FrequencyQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class ConsistencyCheck {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final int numRecords = parameters.getInt("nRec", 500000);
        final double inputFrequency = parameters.getDouble("inputRate", 1000);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 100000);
        final double queryRate = parameters.getDouble("queryRate", 0.1);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final boolean synchronous = parameters.getBoolean("synchronous", false);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);


        NetUtils.launchJobControlServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);

        env.setParallelism(par);

        final Strategy strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.values()[isolationLevelNumber];
        final double startAmount = 100;
        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(strategy, isolationLevel);
        tEnv.setSynchronous(synchronous);
        tEnv.setDurable(false);
        tEnv.setStateServerPoolSize(Runtime.getRuntime().availableProcessors());

        TransferSource transferSource = new TransferSource(numRecords, keySpaceSize, startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        // select * from balances
        tEnv.enableStandardQuerying(
                new FrequencyQuerySupplier(
                        queryID -> new PredicateQuery<>(nameSpace, queryID, new PredicateQuery.SelectAll<>()),
                        queryRate));

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);
        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement, Double> balances = halves.state(
                nameSpace, new OutputTag<Update<Double>>(nameSpace) {
                }, t -> t.f1,
                new StateFunction<Movement, Double>() {
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
                }, partitioning);

        balances.queryResults.addSink(new CheckOnQueryResult(startAmount));

        tEnv.close(balances.leftUnchanged);

        open.wal
                .filter(entry -> entry.f1 != Vote.REPLAY)
                .addSink(new FinishOnCountSink<>(numRecords)).setParallelism(1)
                .name("FinishOnCount");

        env.execute("Consistency check at " + strategy + " - " + isolationLevel);
    }

    private static class CheckOnQueryResult implements SinkFunction<QueryResult> {
        private final double startAmount;

        public CheckOnQueryResult(double startAmount) {
            this.startAmount = startAmount;
        }

        @Override
        public void invoke(QueryResult queryResult) {
            final double[] totalAmount = {0};
            final int[] size = {0};
            queryResult.getResult().forEachRemaining(
                    entry -> {
                        totalAmount[0] += (Double) entry.getValue();
                        size[0]++;
                    }
            );

            if (totalAmount[0] % startAmount != 0) {
                throw new RuntimeException(
                        "Invariant violated: " + totalAmount[0]);
            } else {
                System.out.println("Invariant verified on " + size[0] + " keys");
            }
        }
    }
}
