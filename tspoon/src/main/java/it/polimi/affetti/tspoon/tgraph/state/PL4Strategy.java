package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by affo on 03/08/17.
 */
public class PL4Strategy extends PL3Strategy {

    /**
     * In PL4 we use transaction ids for versioning and not timestamps
     *
     * @param metadata
     * @return
     */
    @Override
    public int getVersionIdentifier(Metadata metadata) {
        return metadata.tid;
    }

    /**
     * Writing is allowed if every version after the watermark is greater than this transaction id.
     * This means that later transactions acted without knowing that this transaction would have been executed.
     * The StrictnessEnforcer will make later transaction REPLAY; however, this transaction is perfectly legal.
     * <p>
     * If we find a version after the watermark that is earlier than this transaction, this transaction
     * must be REPLAYed and wait for its turn.
     *
     * @param metadata
     * @param object
     * @return
     */
    @Override
    public boolean isWritingAllowed(Metadata metadata, Object<?> object) {
        int lastVersion = object.getLastAvailableVersion().version;
        boolean isWriteOK = true;
        for (ObjectVersion<?> v : object.getVersionsWithin(metadata.watermark, lastVersion)) {
            isWriteOK = isWriteOK && v.version >= metadata.tid;
        }
        return isWriteOK;
    }

    /**
     * At PL4, we track every version after the watermark.
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
     *
     * @param metadata
     * @param object
     * @return
     */
    @Override
    public Set<Integer> extractDependencies(Metadata metadata, Object<?> object) {
        int lastVersion = object.getLastAvailableVersion().version;
        Set<Integer> dependencies = new HashSet<>();
        object.getVersionsWithin(metadata.watermark, lastVersion).forEach(v -> dependencies.add(v.version));
        return dependencies;
    }
}
