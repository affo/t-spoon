package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.TWindowFunction;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.backed.Movement;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by affo on 04/08/17.
 */
public class EvaluationGraphComposer {
    public static TransactionEnvironment transactionEnvironment;
    public static final String STATE_BASE_NAME = "state";
    private static int stateCount = 0;
    public static double startAmount = 100d;

    public static void setTransactionEnvironment(TransactionEnvironment transactionEnvironment) {
        EvaluationGraphComposer.transactionEnvironment = transactionEnvironment;
    }

    public static TGraph generateTGraph(
            DataStream<Transfer> transfers, int noStates, int partitioning, boolean seriesOrParallel) {
        TGraph result = new TGraph();
        OpenStream<Transfer> openStream = openTGraph(transfers);
        result.wal = openStream.wal;
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
            out = toSource(transactionEnvironment.close(outOfStateStreams[noStates - 1]).get(0));
        } else {
            List<DataStream<Transfer>> merged = transactionEnvironment.close(outOfStateStreams)
                    .stream().map(EvaluationGraphComposer::toSource).collect(Collectors.toList());
            out = merged.get(0);
            /*
            // It is useless to merge every exit-point's outgoing stream into one stream,
            // because, if I want to synchronize with the transaction closure downstream, it is
            // enough to ingest from 1 stream (the first one for example). Indeed, every buffer function will
            // receive the metadata for a transaction once every vote has been merged.
            // Moreover, without merging, we preserve the number of total records.
            for (DataStream<Transfer> exitPoint : merged.subList(1, merged.size())) {
                out = out.union(exitPoint);
            }
            */
        }

        result.out = out;
        return result;
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
        StateStream<Movement, Double> balances = movements.state(
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

    public static DataStream<Transfer> toSource(DataStream<TransactionResult<Movement>> movements) {
        return movements
                .map(tr -> tr.f2)
                .returns(Movement.class)
                .keyBy(m -> m.f0)
                .flatMap(new InterTGraphMerger()).name("MovementsMerger")
                .returns(TypeInformation.of(new TypeHint<Transfer>() {
                }));
    }

    public static DataStream<TransactionResult<Movement>> closeGraph(TStream<Movement> movements) {
        return transactionEnvironment.close(movements).get(0);
    }

    public static class TGraph {
        private DataStream<Tuple2<Long, Vote>> wal;
        private DataStream<Transfer> out;

        public DataStream<Transfer> getOut() {
            return out;
        }

        public DataStream<Tuple2<Long, Vote>> getWal() {
            return wal;
        }
    }

    /**
     * Output order could be different from input one.
     */
    private static class TransferMerger implements Serializable {
        private Map<Long, Movement> firsts = new HashMap<>();

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

    private static class InterTGraphMerger implements FlatMapFunction<Movement, Transfer> {
        private TransferMerger merger = new TransferMerger();

        @Override
        public void flatMap(Movement movement, Collector<Transfer> collector) throws Exception {
            Transfer transfer = merger.getTransfer(movement);
            if (transfer != null) {
                collector.collect(transfer);
            }
        }
    }
}
