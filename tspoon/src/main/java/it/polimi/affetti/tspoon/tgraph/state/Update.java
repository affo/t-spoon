package it.polimi.affetti.tspoon.tgraph.state;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by affo on 14/07/17.
 */
public class Update<V> extends Tuple3<Long, String, V> {
    public Update() {
    }

    public Update(Long value0, String value1, V value2) {
        super(value0, value1, value2);
    }

    public static <V> Update<V> of(Long tid, String key, V value) {
        return new Update<>(tid, key, value);
    }
}
