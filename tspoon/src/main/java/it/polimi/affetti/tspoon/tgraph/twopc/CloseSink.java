package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.TextClient;
import it.polimi.affetti.tspoon.runtime.TextClientsCache;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 18/07/17.
 */
public class CloseSink extends RichSinkFunction<Metadata> {
    private transient TextClientsCache clients;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.clients = new TextClientsCache();
    }

    @Override
    public void close() throws Exception {
        super.close();
        clients.clear();
    }

    @Override
    public void invoke(Metadata metadata) throws Exception {
        List<TextClient> cohorts = new LinkedList<>();
        for (Address cohort : metadata.cohorts) {
            cohorts.add(clients.getOrCreateClient(cohort));
        }

        String message = metadata.timestamp + "," + metadata.vote.ordinal() + ","
                + metadata.cohorts.size() + "," + metadata.replayCause;

        cohorts.forEach((cohort) -> cohort.text(message));
    }
}
