package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.backed.*;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdateID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by affo on 29/07/17.
 *
 * Class for comparison with transactional environment
 */
public class BankUseCaseNoT {
    public static final String SPU_TRACKING_SERVER_NAME = "spu-tracker";
    public static final double startAmount = 100;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final long bufferTimeout = parameters.getLong("bufferTO", 0);
        final long inputWaitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 10000);

        final int batchSize = parameters.getInt("batchSize", 10000);
        final int resolution = parameters.getInt("resolution", 100);
        final int startInputRate = parameters.getInt("startInputRate", 100);
        final boolean tunable = parameters.getBoolean("tunable", true);

        env.setBufferTimeout(bufferTimeout);

        NetUtils.launchJobControlServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, keySpaceSize, startAmount);
        transferSource.setMicroSleep(inputWaitPeriodMicro);

        // commands
        SinglePartitionUpdate.Command<Double> deposit = new Balances.Deposit();
        SinglePartitionUpdate.Command<Double> withdrawal = new Balances.Withdrawal();

        DataStream<SinglePartitionUpdate> spuStream;
        if (tunable) {
            TunableSource.TunableSPUSource tunableSPUSource = new TunableSource.TunableSPUSource(
                    startInputRate, resolution, batchSize, SPU_TRACKING_SERVER_NAME, "",
                    keySpaceSize
            );

            tunableSPUSource.addCommand(deposit);
            tunableSPUSource.addCommand(withdrawal);
            tunableSPUSource.enableBusyWait();

            spuStream = env.addSource(tunableSPUSource)
                    .name("TunableSPUSource")
                    // parallelism is set to 1 to have a single threaded busy wait
                    .setParallelism(1);
        } else {
            SPUSource spuSource = new SPUSource("", keySpaceSize, batchSize);

            spuSource.addCommand(deposit);
            spuSource.addCommand(withdrawal);

            spuStream = env.addSource(spuSource)
                    .name("TunableSPUSource");
        }

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);

        DataStream<Movement> halves = transfers.flatMap(
                new FlatMapFunction<Transfer, Movement>() {
                    @Override
                    public void flatMap(Transfer transfer, Collector<Movement> collector) throws Exception {
                        collector.collect(transfer.getDeposit());
                        collector.collect(transfer.getWithdrawal());
                    }
                }
        );

        halves.keyBy(movement -> movement.f1);
        spuStream.keyBy(SinglePartitionUpdate::getKey);

        DataStream<Either<TransferID, SinglePartitionUpdateID>> output = halves.connect(spuStream)
                .map(new Balances())
                .name("Balances")
                .setParallelism(partitioning);

        DataStream<TransferID> multiResults = output
                .flatMap((FlatMapFunction<Either<TransferID, SinglePartitionUpdateID>, TransferID>)
                        (o, collector) -> {
                            if (o.isLeft()) {
                                collector.collect(o.left());
                            }
                        }).returns(TransferID.class);
        DataStream<SinglePartitionUpdateID> singleResults = output
                .flatMap((FlatMapFunction<Either<TransferID, SinglePartitionUpdateID>, SinglePartitionUpdateID>)
                        (o, collector) -> {
                            if (o.isRight()) {
                                collector.collect(o.right());
                            }
                        }).returns(SinglePartitionUpdateID.class);

        if (tunable) {
            singleResults
                    .addSink(
                            new FinishOnBackPressure<>(
                                    0.25, batchSize, startInputRate,
                                    resolution, -1, SPU_TRACKING_SERVER_NAME))
                    .name("FinishOnBackPressure")
                    .setParallelism(1);
        } else {
            singleResults
                    .addSink(new FinishOnCountSink<>(batchSize))
                    .name("FinishOnCount")
                    .setParallelism(1);
        }

        env.execute("Pure Flink bank example (no guarantees)");
    }

    private static class Balances implements CoMapFunction<Movement, SinglePartitionUpdate,
            Either<TransferID, SinglePartitionUpdateID>> {
        private final Map<String, Double> balances = new HashMap<>();

        @Override
        public Either<TransferID, SinglePartitionUpdateID> map1(Movement movement) throws Exception {
            String key = movement.f1;
            Double amount = balances.getOrDefault(key, 0.0);
            amount += movement.f2;
            balances.put(key, amount);
            return Either.Left(movement.f0);
        }

        @Override
        public Either<TransferID, SinglePartitionUpdateID> map2(SinglePartitionUpdate spu) throws Exception {
            String key = spu.getKey();
            Double amount = balances.getOrDefault(key, 0.0);
            SinglePartitionUpdate.Command<Double> command = spu.command;
            Double updatedValue = command.apply(amount);
            balances.put(key, updatedValue);
            return Either.Right(spu.id);
        }

        static class Deposit implements SinglePartitionUpdate.Command<Double> {
            private Random random = new Random();

            private double getAmount() {
                return Math.ceil(random.nextDouble() * startAmount);
            }

            @Override
            public Double apply(Double balance) {
                return balance + getAmount();
            }
        }

        static class Withdrawal implements SinglePartitionUpdate.Command<Double> {
            private Random random = new Random();

            private double getAmount() {
                return Math.ceil(random.nextDouble() * startAmount);
            }

            @Override
            public Double apply(Double balance) {
                return balance - getAmount();
            }
        }
    }
}
