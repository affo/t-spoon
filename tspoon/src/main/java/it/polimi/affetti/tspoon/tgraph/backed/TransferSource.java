package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.common.ControlledSource;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 26/07/17.
 */
public class TransferSource extends ControlledSource<Transfer> {
    private final int globalLimit;
    private int limit, noAccounts;
    private double startAmount;
    private List<Transfer> elements;
    private long microSleep = 0L;

    public TransferSource(int limit, int noAccounts, double startAmount) {
        this.globalLimit = limit;
        this.noAccounts = noAccounts;
        this.startAmount = startAmount;
    }

    public TransferSource(Transfer... elements) {
        this.elements = Arrays.asList(elements);
        this.globalLimit = elements.length;
    }

    public void setMicroSleep(long microSleep) {
        this.microSleep = microSleep;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.limit = globalLimit / numberOfTasks;

        if (taskId == 0) {
            this.limit += globalLimit % numberOfTasks;
        }
    }

    @Override
    public void run(SourceContext<Transfer> sourceContext) throws Exception {
        int limit = elements != null ? elements.size() : this.limit;
        for (int i = 0; i < limit && !stop; i++) {
            Transfer transfer;
            if (elements != null) {
                transfer = elements.get(i);
            } else {
                transfer = Transfer.generateTransfer(new TransferID(0, (long) i), noAccounts, startAmount);
            }
            sourceContext.collect(transfer);
            if (microSleep > 0) {
                TimeUnit.MICROSECONDS.sleep(microSleep);
            }
        }

        waitForFinish();
    }
}
