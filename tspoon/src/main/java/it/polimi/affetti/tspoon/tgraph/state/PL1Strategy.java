package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.db.Object;

/**
 * Created by affo on 18/07/17.
 */
public class PL1Strategy extends PL0Strategy {
    @Override
    public boolean isWritingAllowed(Metadata metadata, Object<?> object) {
        int lastVersion = object.getLastAvailableVersion().version;
        return metadata.timestamp > lastVersion;
    }
}
