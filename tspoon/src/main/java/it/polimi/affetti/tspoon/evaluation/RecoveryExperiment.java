package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferSource;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Created by affo on 29/07/17.
 */
public class RecoveryExperiment {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final double inputFrequency = parameters.getDouble("inputRate", 100);
        final long waitPeriodMicro = Evaluation.getWaitPeriodInMicroseconds(inputFrequency);

        // side effect on params
        NetUtils.launchJobControlServer(parameters);
        NetUtils.launchWALServer(parameters, config);
        StreamExecutionEnvironment env = config.getFlinkEnv();
        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);
        tEnv.enableDurability();

        final String nameSpace = "balances";

        TransferSource transferSource = new TransferSource(
                Integer.MAX_VALUE, config.keySpaceSize, EvalConfig.startAmount);
        transferSource.setMicroSleep(waitPeriodMicro);

        DataStream<Transfer> transfers = env.addSource(transferSource)
                .slotSharingGroup(config.sourcesSharingGroup)
                .setParallelism(1);
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

        tEnv.close(balances.leftUnchanged);

        env.execute("Recovery experiment");
    }
}
