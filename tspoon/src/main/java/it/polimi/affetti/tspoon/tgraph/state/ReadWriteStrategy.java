package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.io.Serializable;

/**
 * Created by affo on 18/07/17.
 */
public interface ReadWriteStrategy extends Serializable {
    boolean isReadOK(Metadata metadata, int lastVersion);

    <V> ObjectVersion<V> extractVersion(Metadata metadata, Object<V> versions);

    boolean isWriteOK(Metadata metadata, int lastVersion);
}
