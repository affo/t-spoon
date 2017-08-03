package it.polimi.affetti.tspoon.tgraph.query;

import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 07/06/17.
 */
public class FrequencyQuerySupplier implements QuerySupplier {
    private QuerySupplier wrapped;
    private long waitingIntervalMicro;

    public FrequencyQuerySupplier(QuerySupplier wrapped, double queriesPerSecond) {
        this.wrapped = wrapped;
        this.waitingIntervalMicro = queriesPerSecond == Double.MAX_VALUE ?
                0 :
                (long) ((1.0 / queriesPerSecond) * 1000000);
    }

    @Override
    public Query getQuery() {
        if (waitingIntervalMicro > 0) {
            try {
                TimeUnit.MICROSECONDS.sleep(waitingIntervalMicro);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while sleeping");
            }
        }

        return wrapped.getQuery();
    }
}
