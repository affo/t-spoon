package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.RecordTracker;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.FrequencyQuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.query.QuerySender;
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
import java.util.stream.IntStream;

/**
 * Created by affo on 29/07/17.
 */
public class TransferTestDrive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final int baseParallelism = 4;
        final int partitioning = 4;
        final double startAmount = 100d;
        final Strategy strategy = Strategy.OPTIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.PL3;
        final boolean useDependencyTracking = true;
        final boolean noContention = false;
        final boolean synchronous = false;
        final boolean durable = true;
        final boolean queryOn = false;

        final boolean printPlan = false;

        if (!printPlan) {
            NetUtils.launchJobControlServer(parameters);
        }

        env.getConfig().setGlobalJobParameters(parameters);

        env.setParallelism(baseParallelism);

        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.setStrategy(strategy);
        tEnv.setIsolationLevel(isolationLevel);
        tEnv.setUseDependencyTracking(useDependencyTracking);
        tEnv.setDeadlockTimeout(1000000L); // making deadlock detector useless
        tEnv.setSynchronous(synchronous);
        tEnv.setDurable(durable);

        // NEW setting pool size
        tEnv.setStateServerPoolSize(4);

        final int numberOfElements = 100000;
        TransferSource transferSource;
        if (noContention) {
            Transfer[] transfers = IntStream.range(0, numberOfElements)
                    .mapToObj(i -> {
                        String from = "a" + (i * 2);
                        String to = "a" + (i * 2 + 1);
                        double amount = 10.0;
                        return new Transfer(new TransferID(0, (long) i), from, to, amount);
                    }).toArray(size -> new Transfer[size]);

            transferSource = new TransferSource(transfers);
        } else {
            transferSource = new TransferSource(numberOfElements, 100000, startAmount);
        }

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);

        //transfers.print();

        transfers = transfers.process(
                new RecordTracker<Transfer, TransferID>("responseTime", true) {
                    @Override
                    public OutputTag<TransferID> createRecordTrackingOutputTag(String label) {
                        return new OutputTag<TransferID>(label) {
                        };
                    }

                    @Override
                    protected TransferID extractId(Transfer element) {
                        return element.f0;
                    }
                });

        OpenStream<Transfer> open;
        // we can check consistency only at level PL3 or PL4
        if (queryOn && (isolationLevel == IsolationLevel.PL3 || isolationLevel == IsolationLevel.PL4)) {
            open = tEnv.open(transfers, new ConsistencyCheck(startAmount));
            // select * from balances
            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(
                            queryID -> new PredicateQuery<>("balances", queryID, new PredicateQuery.SelectAll<>()),
                            1));

        } else {
            open = tEnv.open(transfers);
        }

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
                        return balance >= 0;
                    }

                    @Override
                    public void apply(Movement element, ObjectHandler<Double> handler) {
                        // this is the transaction:
                        // r(x) w(x)
                        handler.write(handler.read() + element.f2);
                    }
                }, partitioning);

        //balances.updates.print();

        DataStream<TransactionResult<Movement>> output = tEnv.close(balances.leftUnchanged).get(0);
        output.process(
                new RecordTracker<TransactionResult<Movement>, TransferID>("responseTime", false) {
                    @Override
                    public OutputTag<TransferID> createRecordTrackingOutputTag(String label) {
                        return new OutputTag<TransferID>(label) {
                        };
                    }

                    @Override
                    protected TransferID extractId(TransactionResult<Movement> element) {
                        return element.f2.f0;
                    }
                })
                .returns(new TypeHint<TransactionResult<Movement>>() {
                });
        //balances.updates.addSink(new MaterializedViewChecker(startAmount, false)).setParallelism(1);

        /*
        TGraphOutput<Movement, Double> tGraphOutput = new TGraphOutput<>(open.watermarks, balances.updates, output);
        ResultUtils.addAccumulator(tGraphOutput.watermarks, "watermarks");
        ResultUtils.addAccumulator(tGraphOutput.updates, "updates");
        */

        open.wal
                .filter(entry -> entry.f1 != Vote.REPLAY)
                .addSink(new FinishOnCountSink<>(numberOfElements)).setParallelism(1)
                .name("FinishOnCount");

        //open.wal.print();
        //open.watermarks.print();

        if (printPlan) {
            String executionPlan = env.getExecutionPlan();
            System.out.println(executionPlan);
            return;
        }

        JobExecutionResult result = env.execute();

        //System.out.println(getWatermarks(result));
        //System.out.println(getUpdates(result));

        Report report = new Report("report");
        report.addAccumulators(result);
        report.addField("parameters", parameters.toMap());
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
        private final Map<String, Double> balances = new HashMap<>();
        private final Map<Integer, String> incomplete = new HashMap<>();
        private final boolean verbose;

        public MaterializedViewChecker(double startAmount, boolean verbose) {
            this.startAmount = startAmount;
            this.verbose = verbose;
        }

        @Override
        public void invoke(Update<Double> update) throws Exception {
            balances.put(update.f1, update.f2);

            if (incomplete.containsKey(update.f0)) {
                incomplete.remove(update.f0);
            } else {
                incomplete.put(update.f0, update.f1);
            }

            // check on consistent cut
            double total = balances.entrySet().stream()
                    .filter(e -> !incomplete.containsValue(e.getKey())).mapToDouble(Map.Entry::getValue).sum();
            if (total % startAmount != 0) {
                throw new RuntimeException(
                        "Invariant violated on transaction " + update.f0 + ": " + total);
            }

            if (verbose) {
                System.out.println("Check OK: " + total);
            }
        }
    }

    private static class ConsistencyCheck implements QuerySender.OnQueryResult {
        private final double startAmount;

        public ConsistencyCheck(double startAmount) {
            this.startAmount = startAmount;
        }

        @Override
        public void accept(QueryResult queryResult) {
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
