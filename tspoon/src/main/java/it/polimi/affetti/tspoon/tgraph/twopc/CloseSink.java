package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.StringClientsCache;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Collections;

/**
 * Created by affo on 18/07/17.
 */
public class CloseSink extends RichSinkFunction<Metadata> {
    private transient StringClientsCache clients;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.clients = new StringClientsCache();
    }

    @Override
    public void close() throws Exception {
        super.close();
        clients.clear();
    }

    @Override
    public void invoke(Metadata metadata) throws Exception {
        int dependency;
        if (metadata.dependencyTracking.isEmpty()) {
            dependency = -1;
        } else {
            // TODO could be min or max...
            dependency = Collections.max(metadata.dependencyTracking);
        }

        String message = CloseTransactionNotification.serialize(
                metadata.timestamp,
                metadata.vote,
                metadata.cohorts.size(),
                dependency, ""
        );

        for (Address cohort : metadata.cohorts) {
            clients.getOrCreateClient(cohort).send(message);
        }
    }
}
