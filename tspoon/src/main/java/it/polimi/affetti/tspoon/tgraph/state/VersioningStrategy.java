package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

import java.io.Serializable;

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
    <V> ObjectHandler<V> readVersion(int tid, int timestamp, int watermark, Object<V> versions);

    boolean canWrite(int tid, int timestamp, int watermark, Object<?> object);

    <V> ObjectVersion<V> installVersion(int tid, int timestamp, Object<V> object, V version);
}
