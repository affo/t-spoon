package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.query.Query;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Created by affo on 15/03/18.
 *
 * Either Query or SinglePartitionUpdate
 */
public class NoConsensusOperation implements Serializable {
    private final Query query;
    private final SinglePartitionUpdate spu;

    public NoConsensusOperation(Query query) {
        this.query = query;
        this.spu = null;
    }

    public NoConsensusOperation(SinglePartitionUpdate spu) {
        this.query = null;
        this.spu = spu;
    }

    public boolean isReadOnly() {
        return query != null;
    }

    public Query getReadOnly() {
        Preconditions.checkState(query != null, "Cannot retrieve Query from Update");
        return query;
    }

    public SinglePartitionUpdate getUpdate() {
        Preconditions.checkState(spu != null, "Cannot retrieve Update from Query");
        return spu;
    }
}
