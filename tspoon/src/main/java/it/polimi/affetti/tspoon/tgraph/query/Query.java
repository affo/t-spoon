package it.polimi.affetti.tspoon.tgraph.query;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by affo on 02/08/17.
 */
public class Query implements Serializable {
    public final String nameSpace;
    public final Set<String> keys = new HashSet<>();
    public int watermark;
    public QueryResult result;

    public Query() {
        this("");
    }

    public Query(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public Set<String> getKeys() {
        return keys;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public QueryResult getResult() {
        return result;
    }

    public void addKey(String key) {
        keys.add(key);
    }

    public void accept(QueryVisitor visitor) {
        this.result = visitor.visit(this);
    }

    @Override
    public String toString() {
        return "Query{" +
                "nameSpace='" + nameSpace + '\'' +
                ", keys=" + keys +
                ", watermark=" + watermark +
                '}';
    }
}
