package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 03/08/17.
 */
public class PL4Strategy extends PL3Strategy {

    /**
     * NOTE: Remember that the watermark is a transaction id.
     *
     * Writing is allowed if every version after the watermark had been created by a tnx with tid greater than this transaction's tid.
     * <p>
     * This means that later transactions acted without knowing that this transaction would have been executed.
     * The StrictnessEnforcer will make later transaction REPLAY; however, this transaction is perfectly legal.
     * <p>
     * If we find a version after the watermark that is earlier than this transaction, this transaction
     * must be REPLAYed and wait for its turn.
     */
    @Override
    public boolean canWrite(int tid, int timestamp, int watermark, Object<?> object) {
        for (ObjectVersion<?> version : object.getVersionsAfter(watermark)) {
            // version.version is the same as version.createdBy...
            if (version.version < tid) {
                return false;
            }
        }

        return true;
    }

    @Override
    public <V> ObjectVersion<V> installVersion(int tid, int timestamp, Object<V> object, V version) {
        // we use transaction ids for versioning
        return object.addVersion(tid, tid, version);
    }
}
