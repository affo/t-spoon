package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.RecordTracker;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;

/**
 * Created by affo on 05/08/17.
 */
public class EndToEndTracker extends RecordTracker<Transfer> {
    public static final String metricName = "end2end";

    public EndToEndTracker(boolean isBegin) {
        super(metricName, isBegin);
    }

    @Override
    protected long extractId(Transfer transfer) {
        return transfer.f0;
    }
}
