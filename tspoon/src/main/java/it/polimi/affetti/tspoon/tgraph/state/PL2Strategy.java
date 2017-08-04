package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 18/07/17.
 */
public class PL2Strategy extends PL1Strategy {

    @Override
    public <V> ObjectVersion<V> extractObjectVersion(Metadata metadata, Object<V> versions) {
        return versions.getLastVersionBefore(metadata.watermark);
    }
}
