package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.evaluation.UniquelyRepresentableForTracking;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by affo on 18/12/17.
 */
public class QueryResult implements Serializable, UniquelyRepresentableForTracking {
    public final Map<String, Object> result;
    public final QueryID queryID;

    public QueryResult(QueryID queryID) {
        this.queryID = queryID;
        this.result = new HashMap<>();
    }

    public void add(String key, Object partialResult) {
        this.result.put(key, partialResult);
    }

    public void merge(QueryResult other) {
        this.result.putAll(other.result);
    }

    public Iterator<Map.Entry<String, Object>> getResult() {
        return result.entrySet().iterator();
    }

    @Override
    public String toString() {
        return result.toString();
    }

    @Override
    public String getUniqueRepresentation() {
        return queryID.getUniqueRepresentation();
    }
}