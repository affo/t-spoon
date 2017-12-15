package it.polimi.affetti.tspoon.tgraph.query;

import java.util.Map;

/**
 * Created by affo on 02/08/17.
 */
public interface QueryListener {
    Map<String, ?> onQuery(Query query);

    String getNameSpace();
}
