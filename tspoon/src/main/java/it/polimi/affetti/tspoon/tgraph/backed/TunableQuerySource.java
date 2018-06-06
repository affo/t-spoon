package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.evaluation.EvalConfig;
import it.polimi.affetti.tspoon.evaluation.TunableSource;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryID;
import it.polimi.affetti.tspoon.tgraph.query.QuerySupplier;
import it.polimi.affetti.tspoon.tgraph.query.RandomQuerySupplier;

public class TunableQuerySource extends TunableSource<Query> {
    private transient QuerySupplier supplier;
    private final int keyspaceSize, averageQuerySize;
    private final String namespace;
    private int stdDevQuerySize;

    public TunableQuerySource(EvalConfig config, String trackingServerNameForDiscovery,
            String namespace, int keyspaceSize, int averageQuerySize, int stdDevQuerySize) {
        super(config, trackingServerNameForDiscovery);
        this.keyspaceSize = keyspaceSize;
        this.averageQuerySize = averageQuerySize;
        this.stdDevQuerySize = stdDevQuerySize;
        this.namespace = namespace;
    }

    @Override
    protected Query getNext(int count) {
        if (supplier == null) {
            supplier = new RandomQuerySupplier(
                    namespace, taskNumber, Transfer.KEY_PREFIX, keyspaceSize, averageQuerySize, stdDevQuerySize);
        }

        return supplier.getQuery(new QueryID(taskNumber, (long) count));
    }
}