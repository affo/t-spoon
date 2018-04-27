package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 20/07/17.
 */
public class PL0Strategy implements VersioningStrategy {
    @Override
    public <V> ObjectHandler<V> readVersion(long tid, long timestamp, long watermark, Object<V> versions) {
        return versions.readLastVersionBefore(timestamp);
    }

    @Override
    public boolean canWrite(long tid, long timestamp, long watermark, Object<?> object) {
        return true;
    }

    @Override
    public <V> ObjectVersion<V> installVersion(long tid, long timestamp, Object<V> object, V version) {
        return object.addVersion(tid, timestamp, version);
    }
}
