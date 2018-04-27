package it.polimi.affetti.tspoon.tgraph.twopc;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;

/**
 * Created by affo on 16/01/18.
 */
public class DependencyTracker implements Serializable {
    // tid -> dependent tids (if mapping t1 -> [t2 ... tN] is present, this means that t2 ... tN depends on t1)
    private Map<Long, List<Long>> dependencyTracking = new HashMap<>();
    // t1 -> t2 means that t1 is in dependencyTracking.get(t2)
    private Map<Long, Long> dependentMapping = new HashMap<>();

    public void addDependency(long hasDependent, long dependsOn) {
        Preconditions.checkArgument(dependsOn > hasDependent,
                "Dependent transaction must be newer: " + dependsOn + " < " + hasDependent);

        Long dependenciesLocation = dependentMapping.get(hasDependent);
        List<Long> dependencies;
        if (dependenciesLocation != null) {
            dependencies = dependencyTracking.get(dependenciesLocation);
        } else {
            dependenciesLocation = hasDependent;
            dependencies = dependencyTracking.computeIfAbsent(hasDependent, rc -> new LinkedList<>());
        }

        dependencies.add(dependsOn);
        dependentMapping.put(dependsOn, dependenciesLocation);

        // merge dependencies
        List<Long> previousDependencies = dependencyTracking.remove(dependsOn);
        if (previousDependencies != null) {
            for (long previousDependency : previousDependencies) {
                dependencies.add(previousDependency);
                dependentMapping.put(previousDependency, dependenciesLocation);
            }
        }

        Collections.sort(dependencies);
    }

    /**
     *
     * @param tid
     * @return the tid unlocked by the satisfaction of the dependency. Null if no transaction has been unlocked
     */
    public Long satisfyDependency(long tid) {
        List<Long> dependencies = dependencyTracking.remove(tid);

        Long unlocked = null;
        if (dependencies != null) {
            unlocked = dependencies.remove(0);
            dependentMapping.remove(unlocked);

            if (!dependencies.isEmpty()) {
                dependencyTracking.put(unlocked, dependencies);

                // update locations
                for (long dependency : dependencies) {
                    dependentMapping.put(dependency, unlocked);
                }
            }
        }

        return unlocked;
    }
}
