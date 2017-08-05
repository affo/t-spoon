package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.common.ControlledSource;
import it.polimi.affetti.tspoon.common.RandomProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 26/07/17.
 */
public class TransferSource extends ControlledSource<Transfer> {
    private int limit, noAccounts;
    private double startAmount;
    private Random rand;
    private List<Transfer> elements;
    private long microSleep = 0L;
    public final int noElements;

    public TransferSource(int limit, int noAccounts, double startAmount) {
        this.limit = limit;
        this.noElements = limit;
        this.noAccounts = noAccounts;
        this.startAmount = startAmount;
        this.rand = RandomProvider.get();
    }

    public TransferSource(Transfer... elements) {
        this.elements = Arrays.asList(elements);
        this.noElements = elements.length;
    }

    public void setMicroSleep(long microSleep) {
        this.microSleep = microSleep;
    }

    private Transfer generateTransfer(long id) {
        String from = "a" + rand.nextInt(noAccounts);
        String to;
        do {
            to = "a" + rand.nextInt(noAccounts);
        } while (from.equals(to));
        Double amount = rand.nextDouble() * startAmount;

        return new Transfer(id, from, to, amount);
    }

    @Override
    public void run(SourceContext<Transfer> sourceContext) throws Exception {
        int limit = elements != null ? elements.size() : this.limit;
        for (int i = 0; i < limit && !stop; i++) {
            Transfer transfer;
            if (elements != null) {
                transfer = elements.get(i);
            } else {
                transfer = generateTransfer(i);
            }
            sourceContext.collect(transfer);
            if (microSleep > 0) {
                TimeUnit.MICROSECONDS.sleep(microSleep);
            }
        }

        waitForFinish();
    }
}
