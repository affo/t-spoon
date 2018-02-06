package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.*;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class QueryEvaluation {
    public static final String QUERY_TRACKING_SERVER_NAME = "query-tracker";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final int keySpaceSize = parameters.getInt("ks", 100000);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final boolean synchronous = parameters.getBoolean("synchronous", false);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);

        final double queryPerc = parameters.getDouble("queryPerc", 0.01);
        final int batchSize = parameters.getInt("batchSize", 10000);
        final int resolution = parameters.getInt("resolution", 100);
        final int startInputRate = parameters.getInt("startInputRate", 100);

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

        // TODO implement insert INTO phase?
        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, keySpaceSize, startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        int noKeys = (int) (keySpaceSize * queryPerc);
        QuerySupplier querySupplier = queryID ->
                new RandomQuery(nameSpace, queryID, noKeys);

        TunableSource.TunableQuerySource tunableQuerySource = new TunableSource.TunableQuerySource(
                startInputRate, resolution, batchSize, QUERY_TRACKING_SERVER_NAME, querySupplier);
        DataStream<Query> queries = env.addSource(tunableQuerySource).name("TunableQuerySource");
        DataStream<MultiStateQuery> msQueries = queries.map(q -> {
            MultiStateQuery multiStateQuery = new MultiStateQuery();
            multiStateQuery.addQuery(q);
            return multiStateQuery;
        }).name("ToMultiStateQuery");

        tEnv.enableCustomQuerying(msQueries);

        DataStream<Transfer> transfers = env.addSource(transferSource).setParallelism(1);
        OpenStream<Transfer> open = tEnv.open(transfers);

        open.queryResults
                .map(qr -> qr.queryID).returns(QueryID.class)
                .addSink(
                        new FinishOnBackPressure<>(
                                0.25, batchSize, startInputRate,
                                resolution, -1, QUERY_TRACKING_SERVER_NAME))
                .name("FinishOnBackPressure")
                .setParallelism(1);

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


        tEnv.close(balances.leftUnchanged);

        env.execute("Query evaluation at " + strategy + " - " + isolationLevel);
    }
}
