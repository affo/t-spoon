package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.common.ControlledSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 26/07/17.
 */
public class TransferSource extends ControlledSource<Transfer>
        implements ListCheckpointed<Integer> {
    private final int globalLimit;
    private int limit, noAccounts;
    private double startAmount;
    private List<Transfer> elements;
    private long microSleep = 0L;

    private int offset = 0;
    private int count = 0;

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

        // discard the first `offset` elements
        while (count < offset) {
            Transfer.generateTransfer(
                    new TransferID(taskId, (long) count), noAccounts, startAmount);
            count++;
        }
    }

    @Override
    public void run(SourceContext<Transfer> sourceContext) throws Exception {
        Object lock = sourceContext.getCheckpointLock();

        int limit = elements != null ? elements.size() : this.limit;
        while (count < limit && !stop) {
            Transfer transfer;
            if (elements != null) {
                transfer = elements.get(count);
            } else {
                transfer = Transfer.generateTransfer(
                        new TransferID(taskId, (long) count), noAccounts, startAmount);
            }

            synchronized (lock) {
                sourceContext.collect(transfer);
                count++;
            }

            if (microSleep > 0) {
                TimeUnit.MICROSECONDS.sleep(microSleep);
            }
        }

        waitForFinish();
    }

    @Override
    public List<Integer> snapshotState(long checkpointID, long checkpointTS) throws Exception {
        return Collections.singletonList(count);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        for (Integer offset : state) {
            this.offset = offset;
        }
    }
}
