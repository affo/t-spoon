package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static it.polimi.affetti.tspoon.tgraph.Vote.COMMIT;

/**
 * Created by affo on 29/07/17.
 *
 * Class for comparison with transactional environment
 */
public class BankUseCaseNoT {
    public static final String RECORD_TRACKING_SERVER_NAME = "request-tracker";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        // defaults to max throughput
        final double inputFrequency = parameters.getDouble("inputRate", -1);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int runtimeSeconds = parameters.getInt("runtimeSeconds", 180);
        final int transientSpan = parameters.getInt("transient", 10);
        final TransientPeriod transientPeriod = new TransientPeriod(transientSpan);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        TransferSource transferSource = new TransferSource(
                Integer.MAX_VALUE, config.keySpaceSize, EvalConfig.startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        SingleOutputStreamOperator<Transfer> transfers = env.addSource(transferSource);
        config.addToSourcesSharingGroup(transfers, "TransferSource")
                .slotSharingGroup(EvalConfig.sourceSharingGroup)
                .setParallelism(config.sourcePar);

        transfers
                .map(t -> t.f0)
                .returns(TransferID.class)
                .slotSharingGroup("default")
                .addSink(new LatencyTrackerStart<>(RECORD_TRACKING_SERVER_NAME, transientPeriod))
                .name("LatencyTrackerStart")
                .setParallelism(1);

        transfers
                .slotSharingGroup("default")
                .addSink(new JobTerminator<>(transientPeriod, runtimeSeconds))
                .name("JobTerminator")
                .setParallelism(1);

        DataStream<Movement> halves = transfers.flatMap(
                new FlatMapFunction<Transfer, Movement>() {
                    @Override
                    public void flatMap(Transfer transfer, Collector<Movement> collector) throws Exception {
                        collector.collect(transfer.getDeposit());
                        collector.collect(transfer.getWithdrawal());
                    }
                }
        ).slotSharingGroup("default");
        halves = halves.keyBy(movement -> movement.f1);

        DataStream<TransactionResult> partialResults = halves
                .map(new Balances())
                .name("Balances")
                .setParallelism(config.partitioning);

        DataStream<TransactionResult> output = partialResults
                .keyBy(new KeySelector<TransactionResult, TransferID>() {
                    @Override
                    public TransferID getKey(TransactionResult transactionResult) throws Exception {
                        return (TransferID) transactionResult.f2;
                    }
                }).flatMap(new MergeMovements()).name("MergeMovements");

        output
                .addSink(
                        new ThroughputMeter<>("throughput", transientPeriod))
                .name("ThroughputMeter")
                .setParallelism(1);

        output
                .map(tr -> (TransferID) tr.f2)
                .returns(TransferID.class)
                .addSink(
                        new LatencyTrackerEnd<>(RECORD_TRACKING_SERVER_NAME, "latency"))
                .name("LatencyTrackerEnd")
                .setParallelism(1);

        env.execute("Pure Flink bank example (no guarantees)");
    }

    public static class Balances implements
            MapFunction<Movement, TransactionResult> {

        private final Map<String, Double> balances = new HashMap<>();

        @Override
        public TransactionResult map(Movement movement) throws Exception {
            String key = movement.f1;
            Double amount = balances.getOrDefault(key, 0.0);
            amount += movement.f2;
            balances.put(key, amount);

            Updates updates = new Updates();
            updates.addUpdate("balances", movement.f1, amount);

            return new TransactionResult(-1L, -1L, movement.f0, COMMIT, updates);
        }
    }

    private static class MergeMovements implements FlatMapFunction<TransactionResult, TransactionResult> {
        private Map<TransferID, TransactionResult> movements = new HashMap<>();

        @Override
        public void flatMap(TransactionResult transactionResult, Collector<TransactionResult> collector) throws Exception {
            TransferID key = (TransferID) transactionResult.f2;
            TransactionResult first = movements.remove(key);
            if (first == null) {
                movements.put(key, transactionResult);
            } else {
                first.f4.merge(transactionResult.f4);
                collector.collect(first);
            }
        }
    }
}
