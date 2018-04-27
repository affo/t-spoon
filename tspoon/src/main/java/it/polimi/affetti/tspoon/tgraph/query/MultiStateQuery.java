package it.polimi.affetti.tspoon.tgraph.query;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by affo on 02/08/17.
 */
public class MultiStateQuery implements Iterable<Query>, Serializable {
    public final Set<Query> queries = new HashSet<>();

    public void addQuery(Query query) {
        queries.add(query);
    }

    public void setWatermark(long watermark) {
        queries.forEach(query -> query.watermark = watermark);
    }

    @Override
    public Iterator<Query> iterator() {
        return queries.iterator();
    }
}
