package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 20/07/17.
 */
public class PL0Strategy implements VersioningStrategy {
    @Override
    public <V> ObjectVersion<V> readVersion(int tid, int timestamp, int watermark, Object<V> versions) {
        return versions.getLastVersionBefore(timestamp);
    }

    @Override
    public boolean canWrite(int tid, int timestamp, int watermark, Object<?> object) {
        return true;
    }

    @Override
    public <V> ObjectVersion<V> installVersion(int tid, int timestamp, Object<V> object, V version) {
        return object.addVersion(tid, timestamp, version);
    }
}
