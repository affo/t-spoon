package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by affo on 18/07/17.
 * <p>
 * The only versions a transaction can access are previous to its timestamp:
 * the timestamp, indeed, represents a total order on object versions.
 * <p>
 * At PL0 you can read the previous version (before yours) and write when you want.
 * At PL1 you can read the previous version but you can write only if your timestamp
 * is greater than the last version (avoid write cycles).
 * At PL2 you can read the last COMMITTED version and write as in PL1.
 * At PL3 you can read as in PL2, but you can write only if the last version is known to be finished.
 */
public interface VersioningStrategy extends Serializable {
    <V> ObjectVersion<V> extractObjectVersion(Metadata metadata, Object<V> versions);

    boolean isWritingAllowed(Metadata metadata, Object<?> object);

    Set<Integer> extractDependencies(Metadata metadata, Object<?> object);
}
