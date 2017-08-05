package it.polimi.affetti.tspoon.evaluation;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by affo on 05/08/17.
 */
public class SkipFirstN<T> implements FilterFunction<T> {
    private final int n;
    private int count = 0;

    public SkipFirstN(int n) {
        this.n = n;
    }

    @Override
    public boolean filter(T t) throws Exception {
        count++;
        return count > n;
    }
}
