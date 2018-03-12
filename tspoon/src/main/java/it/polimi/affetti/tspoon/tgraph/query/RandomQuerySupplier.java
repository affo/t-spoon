package it.polimi.affetti.tspoon.tgraph.query;

import org.apache.flink.util.Preconditions;

import java.util.Random;

/**
 * Created by affo on 07/06/17.
 */
public class RandomQuerySupplier implements QuerySupplier {
    private Random random;
    public final int averageSize, keyspaceSize, stdDev;
    public final String keyPrefix, namespace;

    public RandomQuerySupplier(String namespace, int seed, String keyPrefix, int keyspaceSize,
                               int averageSize, int stdDev) {
        Preconditions.checkArgument(averageSize < keyspaceSize);

        this.random = new Random(seed);
        this.averageSize = averageSize;
        this.keyspaceSize = keyspaceSize;
        this.keyPrefix = keyPrefix;
        this.namespace = namespace;
        this.stdDev = stdDev;
    }

    @Override
    public Query getQuery(QueryID queryID) {
        Query query = new Query(namespace, queryID);
        int size = (int) Math.round(random.nextGaussian() * stdDev + averageSize);

        // bound size
        if (size < 1) {
            size = 1;
        }

        if (size > keyspaceSize) {
            size = keyspaceSize;
        }

        random.ints(0, keyspaceSize).distinct().limit(size)
                .boxed()
                .forEach(i -> query.addKey(keyPrefix + i));
        return query;
    }
}
