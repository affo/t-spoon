package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.util.Collections;
import java.util.Set;

/**
 * Created by affo on 20/07/17.
 */
public class PL0Strategy implements VersioningStrategy {
    @Override
    public <V> ObjectVersion<V> extractObjectVersion(int tid, int timestamp, int watermark, Object<V> versions) {
        return versions.getLastVersionBefore(timestamp);
    }

    @Override
    public boolean isWritingAllowed(int tid, int timestamp, int watermark, Object<?> object) {
        return true;
    }

    @Override
    public Set<Integer> extractDependencies(int tid, int timestamp, int watermark, Object<?> object) {
        return Collections.singleton(object.getLastAvailableVersion().version);
    }
}
