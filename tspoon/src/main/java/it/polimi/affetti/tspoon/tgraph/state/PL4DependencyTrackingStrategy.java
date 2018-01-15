package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.Transaction;

/**
 * Created by affo on 11/01/18.
 *
 * At PL4 we are interested in tracking dependencies with later transactions that happened earlier
 * in processing time. That's why we add `createdBy` instead of `version` to the dependency set.
 * <p>
 * NOTE that we must track every dependency to make the StrictnessEnforcer make correct decisions.
 * We provide a concrete example to better illustrate the problem; take 3 transactions in the bank
 * transfer example:
 * - T1 performs a transfer from A to B
 * - T2 performs a deposit to A
 * - T3 performs a deposit to B
 * T2 and T3 have no conflict (they do not share any object), while they both have a conflict with T1.
 * If T2 tries to commit before T1, T1 will track T2's version and the same for T3. The problem is that if
 * we reduce the dependencyTracking using a maximum/minimum function we will erase one of T2 or T3 and
 * the StrictnessEnforcer will not abort one of the two, making them commit before T1.
 */
public class PL4DependencyTrackingStrategy implements DependencyTrackingStrategy {
    private final DependencyTrackingStrategy wrapped;

    public PL4DependencyTrackingStrategy(DependencyTrackingStrategy wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public <T> void updateDependencies(Transaction<T> transaction, Object<T> object) {
        // normal dependencies
        wrapped.updateDependencies(transaction, object);

        // forward dependencies
        object.getVersionsAfter(transaction.watermark)
                .forEach(v -> {
                    if (v.createdBy > transaction.tid) {
                        // here's the trick
                        transaction.addDependency(-v.createdBy);
                    }
                });
    }
}
