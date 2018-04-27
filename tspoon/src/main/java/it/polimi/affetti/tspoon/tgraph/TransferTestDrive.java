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
import it.polimi.affetti.tspoon.tgraph.query.*;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
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
        final boolean queryOn = true;

        final boolean printPlan = false;

        if (!printPlan) {
            NetUtils.launchJobControlServer(parameters);
        }

        env.getConfig().setGlobalJobParameters(parameters);

        env.setParallelism(baseParallelism);

        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(strategy, isolationLevel);
        tEnv.setUseDependencyTracking(useDependencyTracking);
        tEnv.setSynchronous(synchronous);

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

        if (queryOn) {
            QuerySupplier predicateQuerySupplier = queryID -> new PredicateQuery<>(
                    "balances", queryID, new SelectLessThan(50.0)
            );

            QuerySupplier singleKeyQuerySupplier = queryID -> {
                Query q = new Query("balances", queryID);
                q.addKey("a42");
                return q;
            };

            tEnv.enableStandardQuerying(
                    new FrequencyQuerySupplier(singleKeyQuerySupplier, 1));
            tEnv.setOnQueryResult(new QueryResultMerger.PrintQueryResult());
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

        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                "balances", t -> t.f1,
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

        DataStream<TransactionResult> output = tEnv.close(balances.leftUnchanged);
        output.process(
                new RecordTracker<TransactionResult, TransferID>("responseTime", false) {
                    @Override
                    public OutputTag<TransferID> createRecordTrackingOutputTag(String label) {
                        return new OutputTag<TransferID>(label) {
                        };
                    }

                    @Override
                    protected TransferID extractId(TransactionResult result) {
                        Transfer transfer = (Transfer) result.f2;
                        return transfer.f0;
                    }
                })
                .returns(new TypeHint<TransactionResult>() {
                })
                .addSink(new FinishOnCountSink<>(numberOfElements)).setParallelism(1)
                .name("FinishOnCount");

        // Print output
        //output.print();

        if (printPlan) {
            String executionPlan = env.getExecutionPlan();
            System.out.println(executionPlan);
            return;
        }

        JobExecutionResult result = env.execute();

        Report report = new Report("report");
        report.addAccumulators(result);
        report.addField("parameters", parameters.toMap());
        report.updateField("parameters", "strategy", strategy);
        report.updateField("parameters", "isolationLevel", isolationLevel);
        report.updateField("parameters", "dependencyTracking", useDependencyTracking);
        report.writeToFile();
    }

    private static class SelectLessThan implements PredicateQuery.QueryPredicate<Double> {
        private final double threshold;

        public SelectLessThan(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public boolean test(Double balance) {
            return balance < threshold;
        }
    }
}
