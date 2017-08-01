package it.polimi.affetti.tspoon.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by affo on 26/07/17.
 */
public class AccumulatingSink<T> extends RichSinkFunction<T> {
    private StreamAccumulator<T> actual;
    private String accumulatorName;

    public AccumulatingSink(String accumulatorName) {
        this.accumulatorName = accumulatorName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.actual = new StreamAccumulator<>();
        getRuntimeContext().addAccumulator(accumulatorName, this.actual);
    }

    @Override
    public void invoke(T event) throws Exception {
        actual.add(event);
    }
}
