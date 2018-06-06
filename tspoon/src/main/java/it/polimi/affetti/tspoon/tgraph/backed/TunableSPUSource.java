package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.evaluation.EvalConfig;
import it.polimi.affetti.tspoon.evaluation.TunableSource;
import it.polimi.affetti.tspoon.tgraph.state.RandomSPUSupplier;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdateID;

import java.util.Random;

public class TunableSPUSource extends TunableSource<SinglePartitionUpdate> {
    private RandomSPUSupplier supplier;
    private Random random;

    public TunableSPUSource(EvalConfig config, String trackingServerNameForDiscovery, RandomSPUSupplier supplier) {
        super(config, trackingServerNameForDiscovery);
        this.supplier = supplier;

    }

    @Override
    protected SinglePartitionUpdate getNext(int count) {
        if (random == null) {
            random = new Random(taskNumber);
        }

        return supplier.next(new SinglePartitionUpdateID(taskNumber, (long) count), random);
    }
}