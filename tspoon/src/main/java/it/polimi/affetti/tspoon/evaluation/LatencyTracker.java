package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.TimestampTracker;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;

/**
 * Created by affo on 05/08/17.
 */
public class LatencyTracker extends TimestampTracker<Transfer> {
    public static final String metricName = "latency";

    public LatencyTracker(boolean isBegin) {
        super(metricName, isBegin);
    }

    @Override
    protected String extractId(Transfer transfer) {
        return transfer.f0.toString();
    }
}
