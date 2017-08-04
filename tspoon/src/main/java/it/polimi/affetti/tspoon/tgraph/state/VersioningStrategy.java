package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by affo on 18/07/17.
 */
public interface VersioningStrategy extends Serializable {
    int getVersionIdentifier(Metadata metadata);

    <V> ObjectVersion<V> extractObjectVersion(Metadata metadata, Object<V> versions);

    boolean isWritingAllowed(Metadata metadata, Object<?> object);

    Set<Integer> extractDependencies(Metadata metadata, Object<?> object);
}
