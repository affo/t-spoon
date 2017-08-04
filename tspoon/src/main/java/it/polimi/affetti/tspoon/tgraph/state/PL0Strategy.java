package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.util.Collections;
import java.util.Set;

/**
 * Created by affo on 20/07/17.
 */
public class PL0Strategy implements VersioningStrategy {
    @Override
    public int getVersionIdentifier(Metadata metadata) {
        return metadata.timestamp;
    }

    @Override
    public <V> ObjectVersion<V> extractObjectVersion(Metadata metadata, Object<V> versions) {
        return versions.getLastAvailableVersion();
    }

    @Override
    public boolean isWritingAllowed(Metadata metadata, Object<?> object) {
        return true;
    }

    @Override
    public Set<Integer> extractDependencies(Metadata metadata, Object<?> object) {
        return metadata.vote == Vote.REPLAY ?
                Collections.singleton(object.getLastAvailableVersion().version) :
                Collections.emptySet();
    }
}
