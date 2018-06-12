package it.polimi.affetti.tspoon.tgraph.twopc;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by affo on 16/01/18.
 */
public class DependencyTracker implements Serializable {
    // tid -> dependent tids (if mapping t1 -> [t2 ... tN] is present, this means that t2 ... tN depends on t1)
    private Map<Long, Set<Long>> dependencyTracking = new HashMap<>();

    public void addDependency(long hasDependent, long dependsOn) {
        Preconditions.checkArgument(dependsOn > hasDependent,
                "Dependent transaction must be newer: " + dependsOn + " < " + hasDependent);

        Set<Long> dependencies = dependencyTracking
                .computeIfAbsent(hasDependent, k -> new HashSet<>());
        dependencies.add(dependsOn);
    }

    /**
     *
     * @param tid
     * @return the tid unlocked by the satisfaction of the dependency. Null if no transaction has been unlocked
     */
    public Iterable<Long> satisfyDependency(long tid) {
        return dependencyTracking.remove(tid);
    }
}
