package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.common.ControlledSource;
import it.polimi.affetti.tspoon.tgraph.state.RandomSPUSupplier;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdateID;
import org.apache.flink.configuration.Configuration;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 22/03/18.
 *
 * Goes as fast as back-pressure allows
 */
public class SPUSource extends ControlledSource<SinglePartitionUpdate> {
    private transient RandomSPUSupplier supplier;
    private final String namespace;
    private final int keyspaceSize;
    private final List<SinglePartitionUpdate.Command<?>> commands;

    private int count, limit;
    private final int globalLimit;

    public SPUSource(String namespace, int keyspaceSize, int limit) {
        this.namespace = namespace;
        this.keyspaceSize = keyspaceSize;
        this.commands = new LinkedList<>();

        this.count = 0;
        this.globalLimit = limit;
    }

    public void addCommand(SinglePartitionUpdate.Command<?> command) {
        commands.add(command);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.limit = globalLimit / numberOfTasks;

        if (taskId == 0) {
            this.limit += globalLimit % numberOfTasks;
        }

        supplier = new RandomSPUSupplier(namespace, 0, Transfer.KEY_PREFIX,
                keyspaceSize, commands);
    }

    @Override
    public void run(SourceContext<SinglePartitionUpdate> sourceContext) throws Exception {
        if (commands.isEmpty()) {
            throw new RuntimeException("Provide commands please");
        }

        while (count < limit) {
            SinglePartitionUpdate next = supplier.next(new SinglePartitionUpdateID(0, (long) count));
            sourceContext.collect(next);
            count++;
        }

        waitForFinish();
    }
}
