package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.PartitionOrBcastPartitioner;
import it.polimi.affetti.tspoon.evaluation.UniquelyRepresentableForTracking;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public abstract class SinglePartitionUpdate implements PartitionOrBcastPartitioner.Partitionable<String>,
        UniquelyRepresentableForTracking, Serializable {
    public String nameSpace;
    public SinglePartitionUpdateID id;
    public final Command command;

    public SinglePartitionUpdate(String namespace, SinglePartitionUpdateID id, Command command) {
        this.nameSpace = namespace;
        this.id = id;
        this.command = command;
    }

    public abstract <S> Command<S> getCommand();

    public abstract String getKey();


    @Override
    public Set<String> getKeys() {
        return Collections.singleton(getKey());
    }

    @Override
    public void setNumberOfPartitions(int n) {
        // does nothing, always 1 partition
    }

    @Override
    public String getUniqueRepresentation() {
        return id.getUniqueRepresentation();
    }

    @Override
    public String toString() {
        return command.getClass().getSimpleName() + "[" + nameSpace + ", " + id + "]";
    }

    public interface Command<T> extends Function<T, T>, Serializable {

    }
}
