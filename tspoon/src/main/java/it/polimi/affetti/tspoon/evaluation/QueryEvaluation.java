package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.MultiStateQuery;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryID;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class QueryEvaluation {
    public static final String QUERY_TRACKING_SERVER_NAME = "query-tracker";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);
        final int averageQuerySize = parameters.getInt("avg", 1);
        final int stdDevQuerySize = parameters.getInt("stddev", 0);

        Preconditions.checkArgument(averageQuerySize < config.keySpaceSize);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        final String nameSpace = "balances";

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        TransferSource transferSource = new TransferSource(Integer.MAX_VALUE, config.keySpaceSize, config.startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        TunableSource.TunableQuerySource tunableQuerySource = new TunableSource.TunableQuerySource(
                config.startInputRate, config.resolution, config.batchSize, QUERY_TRACKING_SERVER_NAME, nameSpace,
                config.keySpaceSize, averageQuerySize, stdDevQuerySize);
        tunableQuerySource.enableBusyWait();

        SingleOutputStreamOperator<Query> queries = env.addSource(tunableQuerySource);
        queries = config.addToSourcesSharingGroup(queries, "TunableQuerySource");

        SingleOutputStreamOperator<MultiStateQuery> msQueries = queries.map(q -> {
            MultiStateQuery multiStateQuery = new MultiStateQuery();
            multiStateQuery.addQuery(q);
            return multiStateQuery;
        });

        msQueries = config.addToSourcesSharingGroup(msQueries, "ToMultiStateQuery");

        tEnv.enableCustomQuerying(msQueries);

        DataStream<Transfer> transfers = env.addSource(transferSource)
                .slotSharingGroup(config.sourcesSharingGroup).setParallelism(1);

        OpenStream<Transfer> open = tEnv.open(transfers);

        TStream<Movement> halves = open.opened.flatMap(
                (FlatMapFunction<Transfer, Movement>) t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));

        StateStream<Movement> balances = halves.state(
                nameSpace, t -> t.f1,
                new StateFunction<Movement, Double>() {
                    @Override
                    public Double defaultValue() {
                        return EvalConfig.startAmount;
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

        balances.queryResults
                .map(qr -> qr.queryID).returns(QueryID.class)
                .addSink(
                        new FinishOnBackPressure<>(0.10, config.batchSize, config.startInputRate,
                                config.resolution, -1, QUERY_TRACKING_SERVER_NAME))
                .name("FinishOnBackPressure")
                .setParallelism(1);


        tEnv.close(balances.leftUnchanged);

        env.execute("Query evaluation at " + config.strategy + " - " + config.isolationLevel);
    }
}
