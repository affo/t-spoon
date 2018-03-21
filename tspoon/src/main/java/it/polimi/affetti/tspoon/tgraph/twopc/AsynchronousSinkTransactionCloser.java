package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;

import java.util.Collections;

/**
 * Created by affo on 10/11/17.
 */
public class AsynchronousSinkTransactionCloser extends AbstractCloseOperatorTransactionCloser {
    protected AsynchronousSinkTransactionCloser(WALFactory walFactory) {
        super(walFactory);
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

        send(metadata.coordinator, message);
        send(metadata.cohorts, message);
    }
}
