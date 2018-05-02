package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.FrequencyQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class ConsistencyCheck {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final int numRecords = parameters.getInt("nRec", 500000);
        final double inputFrequency = parameters.getDouble("inputRate", 1000);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final double queryRate = parameters.getDouble("queryRate", 0.1);
        final String nameSpace = "balances";
        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = EvalUtils.getFlinkEnv(config);


        // ------------ Topology
        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(config.strategy, config.isolationLevel);
        tEnv.setSynchronous(config.synchronous);
        tEnv.setStateServerPoolSize(Runtime.getRuntime().availableProcessors());
        EvalUtils.setSourcesSharingGroup(tEnv);

        TransferSource transferSource = new TransferSource(numRecords, config.keySpaceSize, EvalUtils.startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        // select * from balances
        tEnv.enableStandardQuerying(
                new FrequencyQuerySupplier(
                        queryID -> new PredicateQuery<>(nameSpace, queryID, new PredicateQuery.SelectAll<>()),
                        queryRate), 1);

        DataStream<Transfer> transfers = env.addSource(transferSource)
                .slotSharingGroup(EvalUtils.sourceSharingGroup)
                .setParallelism(config.sourcePar);
        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new StateFunction<Movement, Double>() {
                    @Override
                    public Double defaultValue() {
                        return EvalUtils.startAmount;
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
                }, config.partitioning);

        balances.queryResults.addSink(new CheckOnQueryResult(EvalUtils.startAmount));

        DataStream<TransactionResult> out = tEnv.close(balances.leftUnchanged);

        out
                .filter(entry -> entry.f3 != Vote.REPLAY)
                .addSink(new FinishOnCountSink<>(numRecords)).setParallelism(1)
                .name("FinishOnCount");

        env.execute("Consistency check at " + config.strategy + " - " + config.isolationLevel);
    }

    public static class CheckOnQueryResult implements SinkFunction<QueryResult> {
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
                System.out.println(">>> Invariant verified on " + size[0] + " keys");
            }
        }
    }
}
