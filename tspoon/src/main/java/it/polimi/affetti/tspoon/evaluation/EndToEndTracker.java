package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.RecordTracker;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 05/08/17.
 */
public class EndToEndTracker extends RecordTracker<Transfer, TransferID> {
    public static final String metricName = "end2end";

    public EndToEndTracker(boolean isBegin) {
        super(metricName, isBegin);
    }

    @Override
    public OutputTag<TransferID> createRecordTrackingOutputTag(String label) {
        return new OutputTag<TransferID>(label) {};
    }

    @Override
    protected TransferID extractId(Transfer transfer) {
        return transfer.f0;
    }
}
