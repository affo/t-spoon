package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.evaluation.EvalConfig;
import it.polimi.affetti.tspoon.evaluation.TunableSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Random;

public class TunableTransferSource extends TunableSource<TransferID> {

    public TunableTransferSource(EvalConfig config, String trackingServerNameForDiscovery) {
        super(config, trackingServerNameForDiscovery);
    }

    @Override
    protected TransferID getNext(int count) {
        return new TransferID(taskNumber, (long) count);
    }


    public static class ToTransfers extends RichMapFunction<TransferID, Transfer> {
        private transient Random random;
        private final int noAccounts;
        private final double startAmount;

        public ToTransfers(int noAccounts, double startAmount) {
            this.noAccounts = noAccounts;
            this.startAmount = startAmount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            random = new Random(getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Transfer map(TransferID tid) throws Exception {
            return Transfer.generateTransfer(tid, noAccounts, startAmount, random);
        }
    }
}
