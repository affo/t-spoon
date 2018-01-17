package it.polimi.affetti.tspoon.metrics;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.io.Serializable;

/**
 * Created by affo on 12/12/17.
 *
 * Should be used by a single-threaded operator, because the value is overridden on merge.
 */
public class SingleValueAccumulator<T extends Serializable> implements SimpleAccumulator<T> {
    private final T startValue;
    private T value;

    public SingleValueAccumulator(T value) {
        this.startValue = value;
        this.value = value;
    }

    @Override
    public void add(T t) {
        value = t;
    }

    // more idiomatic
    public void update(T t) {
        add(t);
    }

    @Override
    public T getLocalValue() {
        return value;
    }

    @Override
    public void resetLocal() {
        value = startValue;
    }

    @Override
    public void merge(Accumulator<T, T> accumulator) {
        this.value = accumulator.getLocalValue();
    }

    @Override
    public Accumulator<T, T> clone() {
        return new SingleValueAccumulator<>(value);
    }
}
