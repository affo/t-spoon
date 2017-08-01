package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 20/07/17.
 */
public class PL0Strategy implements ReadWriteStrategy {
    @Override
    public boolean isReadOK(Metadata metadata, int lastVersion) {
        return true;
    }

    @Override
    public <V> ObjectVersion<V> extractVersion(Metadata metadata, Object<V> versions) {
        return versions.getLastVersionBefore(Integer.MAX_VALUE);
    }

    @Override
    public boolean isWriteOK(Metadata metadata, int lastVersion) {
        return true;
    }
}
