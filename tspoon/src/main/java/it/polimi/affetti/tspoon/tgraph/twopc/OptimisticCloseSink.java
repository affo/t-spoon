package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.TextClient;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Created by affo on 18/07/17.
 */
public class OptimisticCloseSink extends CloseSink<TextClient> {

    @Override
    protected Function<Address, TextClient> clientSupplier() {
        return (a) -> new TextClient(a.ip, a.port);
    }

    @Override
    protected void process(int tid, Vote vote, TextClient coordinator, List<TextClient> cohorts) throws IOException {
        String coordinatorMsg = tid + ", OPT COORDINATOR - " + vote;
        String cohortMsg = tid + "," + vote.ordinal();
        coordinator.text(coordinatorMsg);
        cohorts.forEach((cohort) -> {
            try {
                cohort.text(cohortMsg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
