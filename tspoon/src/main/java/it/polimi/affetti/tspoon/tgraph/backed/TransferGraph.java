package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.common.FinishOnCountSink;
import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * Created by affo on 26/07/17.
 */
public abstract class TransferGraph extends TGraph<Movement, Double> {
    private int statePartitioning;
    private double startAmount;

    public TransferGraph(int statePartitioning, double startAmount,
                         Strategy strategy, IsolationLevel isolationLevel) {
        super(strategy, isolationLevel);
        this.statePartitioning = statePartitioning;
        this.startAmount = startAmount;
    }

    @Override
    protected TGraphOutput<Movement, Double> doDraw(StreamExecutionEnvironment env) {
        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.setStrategy(strategy);
        tEnv.setIsolationLevel(isolationLevel);

        TransferSource transferSource = getTransfers();
        DataStream<Transfer> transfers = env.addSource(transferSource);

        OpenStream<Transfer> open = tEnv.open(transfers);

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
                }, statePartitioning);

        balances.updates.print();

        DataStream<TransactionResult<Movement>> output = tEnv.close(balances.leftUnchanged).get(0);
        output.addSink(new FinishOnCountSink<>(transferSource.noElements)).setParallelism(1);

        return new TGraphOutput<>(open.watermarks, balances.updates, output);
    }

    protected abstract TransferSource getTransfers();
}
