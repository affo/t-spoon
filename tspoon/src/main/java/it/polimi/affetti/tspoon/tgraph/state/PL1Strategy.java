package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;

/**
 * Created by affo on 18/07/17.
 */
public class PL1Strategy extends PL0Strategy {
    @Override
    public boolean canWrite(long tid, long timestamp, long watermark, Object<?> object) {
        long lastVersion = object.getLastAvailableVersion().version;
        return timestamp > lastVersion;
    }
}
