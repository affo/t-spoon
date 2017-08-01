package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Metadata;

/**
 * Created by affo on 18/07/17.
 * <p>
 * It is always possible to read.
 * At PL0 you can read the last version and write when you want.
 * At PL1 you can read the last version but you can write only when your timestamp
 * is greater than the last version (avoid write cycles).
 * At PL2 you can read the last COMMITTED version and write as in PL1.
 * At PL3 you can read as in PL2, but you can write only if the last version is known to be terminated.
 */
public class PL3Strategy extends PL2Strategy {
    @Override
    public boolean isWriteOK(Metadata metadata, int lastVersion) {
        return metadata.watermark >= lastVersion;
    }
}
