package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;

import java.util.Collections;

/**
 * Created by affo on 10/11/17.
 */
public class SynchronousSinkTransactionCloser extends AbstractCloseOperatorTransactionCloser {

    protected SynchronousSinkTransactionCloser(boolean isDurabilityEnabled) {
        super(isDurabilityEnabled);
    }

    @Override
    public void applyProtocolOnMetadata(Metadata metadata) throws Exception {
        int dependency;
        if (metadata.dependencyTracking.isEmpty()) {
            dependency = -1;
        } else {
            dependency = Collections.max(metadata.dependencyTracking);
        }

        String message = CloseTransactionNotification.serialize(
                metadata.tGraphID,
                metadata.timestamp,
                metadata.vote,
                metadata.cohorts.size(),
                dependency
        );

        send(metadata.cohorts, message);
    }
}
