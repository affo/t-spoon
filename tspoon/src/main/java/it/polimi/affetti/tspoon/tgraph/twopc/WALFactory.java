package it.polimi.affetti.tspoon.tgraph.twopc;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by affo on 21/03/18.
 */
public class WALFactory implements Serializable {
    private final boolean isDurabilityEnabled;

    public WALFactory(boolean isDurabilityEnabled) {
        this.isDurabilityEnabled = isDurabilityEnabled;
    }

    public WAL getWAL(ParameterTool params) throws IOException {
        if (isDurabilityEnabled) {
            return WALClient.get(params);
        }

        return new NoWAL();
    }

    public boolean isDurabilityEnabled() {
        return isDurabilityEnabled;
    }
}
