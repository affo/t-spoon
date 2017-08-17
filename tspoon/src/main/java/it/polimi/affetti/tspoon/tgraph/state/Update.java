package it.polimi.affetti.tspoon.tgraph.state;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by affo on 14/07/17.
 */
public class Update<V> extends Tuple4<Integer, Long, String, V> {
    public Update() {
    }

    public Update(Integer value0, Long value1, String value2, V value3) {
        super(value0, value1, value2, value3);
    }

    public static <V> Update<V> of(Integer tid, Long timestamp, String key, V value) {
        return new Update<>(tid, timestamp, key, value);
    }
}
