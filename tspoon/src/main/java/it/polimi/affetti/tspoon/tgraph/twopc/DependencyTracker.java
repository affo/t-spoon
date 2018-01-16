package it.polimi.affetti.tspoon.tgraph.twopc;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;

/**
 * Created by affo on 16/01/18.
 */
public class DependencyTracker implements Serializable {
    // tid -> dependent tids (if mapping t1 -> [t2 ... tN] is present, this means that t2 ... tN depends on t1)
    private Map<Integer, List<Integer>> dependencyTracking = new HashMap<>();
    // t1 -> t2 means that t1 is in dependencyTracking.get(t2)
    private Map<Integer, Integer> dependentMapping = new HashMap<>();

    public void addDependency(int hasDependent, int dependsOn) {
        Preconditions.checkArgument(dependsOn > hasDependent,
                "Dependent transaction must be newer: " + dependsOn + " < " + hasDependent);

        Integer dependenciesLocation = dependentMapping.get(hasDependent);
        List<Integer> dependencies;
        if (dependenciesLocation != null) {
            dependencies = dependencyTracking.get(dependenciesLocation);
        } else {
            dependenciesLocation = hasDependent;
            dependencies = dependencyTracking.computeIfAbsent(hasDependent, rc -> new LinkedList<>());
        }

        dependencies.add(dependsOn);
        dependentMapping.put(dependsOn, dependenciesLocation);

        // merge dependencies
        List<Integer> previousDependencies = dependencyTracking.remove(dependsOn);
        if (previousDependencies != null) {
            for (int tidDependency : previousDependencies) {
                dependencies.add(tidDependency);
                dependentMapping.put(tidDependency, dependenciesLocation);
            }
        }

        Collections.sort(dependencies);
    }

    /**
     *
     * @param tid
     * @return the tid unlocked by the satisfaction of the dependency. Null if no transaction has been unlocked
     */
    public Integer satisfyDependency(int tid) {
        List<Integer> dependencies = dependencyTracking.remove(tid);

        Integer unlocked = null;
        if (dependencies != null) {
            unlocked = dependencies.remove(0);
            dependentMapping.remove(unlocked);

            if (!dependencies.isEmpty()) {
                dependencyTracking.put(unlocked, dependencies);
            }
        }

        return unlocked;
    }
}
