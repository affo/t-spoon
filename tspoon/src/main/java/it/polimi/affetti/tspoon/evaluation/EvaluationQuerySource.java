package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryID;
import it.polimi.affetti.tspoon.tgraph.query.QuerySupplier;

/**
 * Created by affo on 16/03/17.
 */
public class EvaluationQuerySource extends TunableSource<Query> {
    private final QuerySupplier supplier;

    public EvaluationQuerySource(EvalConfig config, String trackingServerNameForDiscovery,
            QuerySupplier querySupplier) {
        super(config, trackingServerNameForDiscovery);
        this.supplier = querySupplier;
    }

    @Override
    protected Query getNext(int count) {
        return supplier.getQuery(new QueryID(taskNumber, (long) count));
    }
}
