package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;

/**
 * Created by affo on 18/07/17.
 */
public class PL1Strategy extends PL0Strategy {
    @Override
    public boolean isWriteOK(Metadata metadata, int lastVersion) {
        return metadata.timestamp > lastVersion;
    }
}
