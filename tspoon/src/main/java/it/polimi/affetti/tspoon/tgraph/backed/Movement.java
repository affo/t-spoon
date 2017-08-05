package it.polimi.affetti.tspoon.tgraph.backed;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by affo on 26/07/17.
 */
public class Movement extends Tuple3<Long, String, Double> {
    public Movement() {
    }

    public Movement(Long id, String from, Double amount) {
        super(id, from, amount);
    }


}
