package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.ClientsCache;
import it.polimi.affetti.tspoon.runtime.AbstractClient;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by affo on 18/07/17.
 */
public abstract class CloseSink<C extends AbstractClient> extends RichSinkFunction<TwoPCData> {
    private transient ClientsCache<C> clients;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.clients = new ClientsCache<>(clientSupplier());
    }

    @Override
    public void close() throws Exception {
        super.close();
        clients.clear();
    }

    @Override
    public void invoke(TwoPCData twoPCData) throws Exception {
        C coordinator = clients.getOrCreateClient(twoPCData.coordinator);
        List<C> cohorts = new LinkedList<>();
        for (Address cohort : twoPCData.cohorts) {
            cohorts.add(clients.getOrCreateClient(cohort));
        }

        process(twoPCData.tid, twoPCData.vote, coordinator, cohorts);
    }

    protected abstract Function<Address, C> clientSupplier();

    protected abstract void process(
            int tid, Vote vote, C coordinator, List<C> cohorts) throws IOException;
}
