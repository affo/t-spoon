package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.TimestampTracker;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.JobControlServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.runtime.TimestampDeltaServer;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.TGraphOutput;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.FrequencyQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.PredefinedQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.RandomQuery;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 29/07/17.
 */
public class TransferTestDrive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setBufferTimeout(0);
        ParameterTool parameters = ParameterTool.fromArgs(args);
        JobControlServer jobControlServer = NetUtils.launchJobControlServer(parameters);
        TimestampDeltaServer timestampDeltaServer = NetUtils.launchTimestampDeltaServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);

        final double startAmount = 100d;
        final Strategy strategy = Strategy.OPTIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.PL3;
        final boolean useDependencyTracking = true;

        TransactionEnvironment tEnv = TransactionEnvironment.get();
        tEnv.setStrategy(strategy);
        tEnv.setIsolationLevel(isolationLevel);
        tEnv.setUseDependencyTracking(useDependencyTracking);

        final int numberOfElements = 1000;
        TransferSource transferSource = new TransferSource(numberOfElements, 1000, startAmount);
        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);

        transfers = transfers.map(
                new TimestampTracker<Transfer>("responseTime", true) {
                    @Override
                    protected String extractId(Transfer element) {
                        return element.f0.toString();
                    }
                });

        OpenStream<Transfer> open = tEnv.open(transfers);

        tEnv.setQuerySupplier(
                new FrequencyQuerySupplier(
                        new PredefinedQuerySupplier(
                                new RandomQuery("balances", 1)), 100));

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement, Double> balances = halves.state(
                "balances", new OutputTag<Update<Double>>("balances") {
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
                        return balance > 0;
                    }

                    @Override
                    public void apply(Movement element, ObjectHandler<Double> handler) {
                        // this is the transaction:
                        // r(x) w(x)
                        handler.write(handler.read() + element.f2);
                    }
                }, 4);

        balances.updates.print();

        DataStream<TransactionResult<Movement>> output = tEnv.close(balances.leftUnchanged).get(0);
        output.map(
                new TimestampTracker<TransactionResult<Movement>>("responseTime", false) {
                    @Override
                    protected String extractId(TransactionResult<Movement> element) {
                        return element.f2.f0.toString();
                    }
                })
                .returns(new TypeHint<TransactionResult<Movement>>() {
                });
        balances.updates.addSink(new MaterializedViewChecker(startAmount)).setParallelism(1);

        TGraphOutput<Movement, Double> tGraphOutput = new TGraphOutput<>(open.watermarks, balances.updates, output);
        //ResultUtils.addAccumulator(tGraphOutput.watermarks, "watermarks");
        //ResultUtils.addAccumulator(tGraphOutput.updates, "updates");

        open.wal.print();
        open.wal
                .filter(entry -> entry.f1 != Vote.REPLAY)
                .addSink(new FinishOnCountSink<>(numberOfElements)).setParallelism(1);

        JobExecutionResult result = env.execute();
        jobControlServer.close();
        timestampDeltaServer.close();

        //System.out.println(getWatermarks(result));
        //System.out.println(getUpdates(result));

        Report report = new Report("report.json");
        report.addAccumulators(result);
        report.addField("parameters", parameters.toMap());
        report.addFields(timestampDeltaServer.getMetrics());
        report.updateField("parameters", "strategy", strategy);
        report.updateField("parameters", "isolationLevel", isolationLevel);
        report.updateField("parameters", "dependencyTracking", useDependencyTracking);
        report.writeToFile();
    }

    /**
     * Materializes the view using the updates and checks that no money has been created nor
     * destroyed in consistent cuts on the update.
     */
    private static class MaterializedViewChecker implements SinkFunction<Update<Double>> {
        private final double startAmount;
        private Map<String, Double> balances = new HashMap<>();
        private Map<Integer, Update<Double>> firsts = new HashMap<>();

        public MaterializedViewChecker(double startAmount) {
            this.startAmount = startAmount;
        }

        private boolean nothingGetsCreatedNorDestroyed() {
            return balances.values().stream().mapToDouble(d -> d).sum() == startAmount * balances.size();
        }

        @Override
        public void invoke(Update<Double> update) throws Exception {
            Update<Double> first = firsts.remove(update.f0);
            if (first != null) {
                balances.put(first.f1, first.f2);
                balances.put(update.f1, update.f2);

                assert nothingGetsCreatedNorDestroyed();
                System.out.println("CHECK PASSED: " + startAmount * balances.size());
            } else {
                firsts.put(update.f0, update);
            }
        }
    }
}
