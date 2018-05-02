package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.TWindowFunction;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.polimi.affetti.tspoon.evaluation.EvalUtils.startAmount;

/**
 * Created by affo on 04/08/17.
 */
public class EvaluationGraphComposer {
    public static TransactionEnvironment transactionEnvironment;
    public static final String STATE_BASE_NAME = "state";
    private static int stateCount = 0;

    public static void setTransactionEnvironment(TransactionEnvironment transactionEnvironment) {
        EvaluationGraphComposer.transactionEnvironment = transactionEnvironment;
    }

    public static DataStream<Transfer> generateTGraph(
            DataStream<Transfer> transfers, int noStates, int partitioning, boolean seriesOrParallel) {
        OpenStream<Transfer> openStream = openTGraph(transfers);
        TStream<Transfer> open = openStream.opened;
        TStream<Movement> source = toMovements(open);

        int i = 0;
        TStream<Movement>[] outOfStateStreams = new TStream[noStates];
        do {
            TStream<Movement> outOfState = addState(source, partitioning);
            outOfStateStreams[i] = outOfState;
            if (seriesOrParallel && i < noStates - 1) {
                source = toMovements(toSource(outOfState));
            }
            i++;
        } while (i < noStates);

        DataStream<Transfer> out;
        if (seriesOrParallel) {
            out = toSource(transactionEnvironment.close(outOfStateStreams[noStates - 1]));
        } else {
            out = toSource(transactionEnvironment.close(outOfStateStreams));
        }

        return out;
    }

    public static OpenStream<Transfer> openTGraph(DataStream<Transfer> transfers) {
        return transactionEnvironment.open(transfers);
    }

    public static TStream<Movement> toMovements(TStream<Transfer> transfers) {
        return transfers.flatMap(t -> Arrays.asList(t.getDeposit(), t.getWithdrawal()));
    }

    public static TStream<Movement> addState(TStream<Movement> movements, int partitioning) {
        String nameSpace = STATE_BASE_NAME + stateCount;
        stateCount++;
        StateStream<Movement> balances = movements.state(
                nameSpace, t -> t.f1,
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
                }, partitioning);

        return balances.leftUnchanged;
    }

    public static TStream<Transfer> toSource(TStream<Movement> movements) {
        return movements.window(new IntraTGraphMerger());
    }

    public static DataStream<Transfer> toSource(DataStream<TransactionResult> movements) {
        return movements
                .map(tr -> (Transfer) tr.f2)
                .returns(Transfer.class);
    }

    public static DataStream<TransactionResult> closeGraph(TStream<Movement> movements) {
        return transactionEnvironment.close(movements);
    }

    /**
     * Output order could be different from input one.
     */
    private static class TransferMerger implements Serializable {
        private Map<TransferID, Movement> firsts = new HashMap<>();

        public Transfer getTransfer(Movement movement) {
            Movement first = firsts.remove(movement.f0);

            if (first != null && !first.equals(movement)) {
                String from = first.f2 < 0 ? first.f1 : movement.f1;
                String to = first.f2 >= 0 ? first.f1 : movement.f1;
                double amount = Math.abs(first.f2);
                return new Transfer(first.f0, from, to, amount);
            }

            firsts.put(movement.f0, movement);
            return null;
        }
    }

    private static class IntraTGraphMerger implements TWindowFunction<Movement, Transfer> {
        private TransferMerger merger = new TransferMerger();

        @Override
        public Transfer apply(List<Movement> batch) {
            Transfer result = null;

            for (Movement movement : batch) {
                result = merger.getTransfer(movement);
            }
            return result;
        }
    }
}
