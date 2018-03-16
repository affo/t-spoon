package it.polimi.affetti.tspoon.tgraph.state;

import java.util.List;
import java.util.Random;

/**
 * Created by affo on 07/06/17.
 */
public class RandomSPUSupplier {
    private Random random;
    public final int keyspaceSize;
    public final String keyPrefix, namespace;
    private final SinglePartitionUpdate.Command<?>[] updates;

    public RandomSPUSupplier(String namespace, int seed, String keyPrefix, int keyspaceSize,
                             List<SinglePartitionUpdate.Command<?>> updates) {

        this.random = new Random(seed);
        this.keyspaceSize = keyspaceSize;
        this.keyPrefix = keyPrefix;
        this.namespace = namespace;
        this.updates = new SinglePartitionUpdate.Command[updates.size()];
        updates.toArray(this.updates);
    }

    public SinglePartitionUpdate next(SinglePartitionUpdateID spuID) {
        SinglePartitionUpdate.Command<?> command = updates[random.nextInt(updates.length)];
        String key = keyPrefix + random.nextInt(keyspaceSize);

        return new SinglePartitionUpdate(namespace, spuID, command) {
            @Override
            public <S> Command<S> getCommand() {
                return command;
            }

            @Override
            public String getKey() {
                return key;
            }
        };
    }
}
