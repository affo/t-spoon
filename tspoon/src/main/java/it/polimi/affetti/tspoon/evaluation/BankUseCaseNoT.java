package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.RPC;
import it.polimi.affetti.tspoon.common.SinglePartitionCommand;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.backed.*;
import it.polimi.affetti.tspoon.tgraph.state.DepositsAndWithdrawalsGenerator;
import it.polimi.affetti.tspoon.tgraph.state.RandomSPUSupplier;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdateID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

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
        final boolean singleSource = parameters.getBoolean("singleSource", false);

        // relevant only if single source
        final boolean managedState = parameters.getBoolean("managedState", false);
        final boolean reducing = parameters.getBoolean("reducing", false);

        env.setBufferTimeout(bufferTimeout);

        NetUtils.launchJobControlServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        DataStream<Movement> halves = null;
        if (!singleSource) {
            TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, keySpaceSize, startAmount);
            transferSource.setMicroSleep(inputWaitPeriodMicro);
            DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);
            halves = transfers.flatMap(
                    new FlatMapFunction<Transfer, Movement>() {
                        @Override
                        public void flatMap(Transfer transfer, Collector<Movement> collector) throws Exception {
                            collector.collect(transfer.getDeposit());
                            collector.collect(transfer.getWithdrawal());
                        }
                    }
            );
            halves = halves.keyBy(movement -> movement.f1);
        }

        RandomSPUSupplier spuSupplier = new DepositsAndWithdrawalsGenerator(
                "balances", Transfer.KEY_PREFIX, keySpaceSize, startAmount
        );

        DataStream<SinglePartitionUpdate> spuStream;
        if (tunable) {
            TunableSource.TunableSPUSource tunableSPUSource = new TunableSource.TunableSPUSource(
                    startInputRate, resolution, batchSize, SPU_TRACKING_SERVER_NAME, spuSupplier
            );
            tunableSPUSource.enableBusyWait();

            spuStream = env.addSource(tunableSPUSource)
                    .name("TunableSPUSource")
                    // parallelism is set to 1 to have a single threaded busy wait
                    .setParallelism(1);
        } else {
            SPUSource spuSource = new SPUSource("", keySpaceSize, batchSize, spuSupplier);

            spuStream = env.addSource(spuSource)
                    .name("ParallelSPUSource"); // runs with default parallelism
        }


        DataStream<SinglePartitionUpdateID> singleResults;
        if (singleSource) {
            if (reducing) {
                singleResults = spuStream
                        .map(spu -> Tuple2.of(0.0, spu))
                        .returns(TypeInformation.of(new TypeHint<Tuple2<Double, SinglePartitionUpdate>>() {
                        }))
                        .keyBy(
                                new KeySelector<Tuple2<Double, SinglePartitionUpdate>, String>() {
                                    @Override
                                    public String getKey(Tuple2<Double, SinglePartitionUpdate> t) throws Exception {
                                        return t.f1.getKey();
                                    }
                                })
                        .reduce(new ReducingBalance()).name("Reducing Balances")
                        .map(t -> t.f1.id)
                        .returns(SinglePartitionUpdateID.class);
            } else {
                singleResults = spuStream
                        .keyBy(SinglePartitionUpdate::getKey)
                        .map(new Balances(managedState))
                        .name("Balances")
                        .setParallelism(partitioning);
            }
        } else {
            DataStream<Either<TransferID, SinglePartitionUpdateID>> output = halves.connect(spuStream)
                    .map(new CoBalances())
                    .name("CoBalances")
                    .setParallelism(partitioning);

            DataStream<TransferID> multiResults = output
                    .flatMap((FlatMapFunction<Either<TransferID, SinglePartitionUpdateID>, TransferID>)
                            (o, collector) -> {
                                if (o.isLeft()) {
                                    collector.collect(o.left());
                                }
                            }).returns(TransferID.class);
            singleResults = output
                    .flatMap((FlatMapFunction<Either<TransferID, SinglePartitionUpdateID>, SinglePartitionUpdateID>)
                            (o, collector) -> {
                                if (o.isRight()) {
                                    collector.collect(o.right());
                                }
                            }).returns(SinglePartitionUpdateID.class);
        }

        if (tunable) {
            singleResults
                    .addSink(
                            new FinishOnBackPressure<>(0.25, batchSize, startInputRate,
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

    private static class StateFunction {
        @SinglePartitionCommand
        public double deposit(double amount, double currentBalance) {
            return currentBalance + amount;
        }

        @SinglePartitionCommand
        public double withdrawal(double amount, double currentBalance) {
            return currentBalance - amount;
        }
    }

    private static StateFunction stateFunction = new StateFunction();

    private static class CoBalances implements
            CoMapFunction<Movement, SinglePartitionUpdate, Either<TransferID, SinglePartitionUpdateID>> {

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

            RPC command = spu.getCommand();
            command.addParam(amount);
            Double updatedValue = (Double) command.call(stateFunction);

            balances.put(key, updatedValue);

            return Either.Right(spu.id);
        }
    }

    private static class Balances extends
            RichMapFunction<SinglePartitionUpdate, SinglePartitionUpdateID> {

        private final Map<String, Double> balances = new HashMap<>();
        private ValueState<Double> managedBalances;
        private final boolean managed;

        public Balances(boolean managed) {
            this.managed = managed;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<Double> sd = new ValueStateDescriptor<>("balances", Double.class);
            managedBalances = getRuntimeContext().getState(sd);
        }

        @Override
        public SinglePartitionUpdateID map(SinglePartitionUpdate spu) throws Exception {
            String key = spu.getKey();
            Double amount;

            if (managed) {
                amount = managedBalances.value();
                if (amount == null) {
                    amount = 0.0;
                }
            } else {
                amount = balances.getOrDefault(key, 0.0);
            }

            RPC command = spu.getCommand();
            command.addParam(amount);
            Double updatedValue = (Double) command.call(stateFunction);

            if (managed) {
                managedBalances.update(updatedValue);
            } else {
                balances.put(key, updatedValue);
            }

            return spu.id;
        }
    }

    private static class ReducingBalance implements ReduceFunction<Tuple2<Double, SinglePartitionUpdate>> {

        @Override
        public Tuple2<Double, SinglePartitionUpdate> reduce(
                Tuple2<Double, SinglePartitionUpdate> acc, Tuple2<Double, SinglePartitionUpdate> newVal) throws Exception {
            // newVal.f0 is useless as acc.f1!
            SinglePartitionUpdate spu = newVal.f1;
            Double balance = acc.f0;

            RPC command = spu.getCommand();
            command.addParam(balance);
            Double updatedValue = (Double) command.call(stateFunction);
            return Tuple2.of(updatedValue, spu);
        }
    }
}
