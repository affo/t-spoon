package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by affo on 18/11/17.
 * <p>
 * The BatchID is a data structure responsible for representing the composed ID of a record.
 * <p>
 * The ID is built by each operator as the records passes throw, from the OpenOperator to the CloseFunction.
 * <p>
 * An operator can generate 1 (in the case of 0, an empty record is generated) or more records from 1 record.
 * When an operator produces a new batch of records from one record, it is supposed to copy the current BatchID of
 * the incoming record and assign it to every record in the new batch. In addition, it enriches the BatchIds of the
 * records in the batch with a new step in the BatchID containing the size of the new batch and the offset of the
 * record in the batch.
 * <p>
 * In the implementation we take into account serialization and the particular use case.
 * Indeed, it would be useless to deep copy the BatchID for every new record in the new batch.
 * The new step is saved in separated fields and it is consolidated upon new step generation.
 * Generating a new step will create a new BatchID that shares the root ID except for the new step introduced.
 */
public final class BatchID implements Serializable, Iterable<Tuple2<Integer, Integer>>, Comparable<BatchID> {
    public LinkedList<Integer> offsets;
    public LinkedList<Integer> sizes;

    public int newOffset, newSize;

    public BatchID() {
        offsets = new LinkedList<>();
        sizes = new LinkedList<>();
    }

    public BatchID(long tid) {
        this((int) tid, 1, new LinkedList<>(), new LinkedList<>());
        consolidate();
    }

    private BatchID(int offset, int size,
                    LinkedList<Integer> offsets, LinkedList<Integer> sizes) {
        this.newOffset = offset;
        this.newSize = size;
        this.offsets = offsets;
        this.sizes = sizes;
    }

    public void consolidate() {
        // consolidate only once
        if (newOffset > 0 && newSize > 0) {
            offsets.add(newOffset);
            sizes.add(newSize);
            newOffset = 0;
            newSize = 0;
        }
    }

    public List<BatchID> addStep(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Illegal size: " + size);
        }

        consolidate();
        List<BatchID> results = new ArrayList<>(size);

        for (int i = 1; i <= size; i++) {
            BatchID id = new BatchID(i, size, offsets, sizes);
            results.add(id);
        }

        return results;
    }

    public void addStepManually(int offset, int size) {
        consolidate();
        offsets.add(offset);
        sizes.add(size);
    }

    public int getNumberOfSteps() {
        return offsets.size() + (newOffset == 0 ? 0 : 1);
    }

    @Override
    public Iterator<Tuple2<Integer, Integer>> iterator() {
        return new BatchIDIterator(this);
    }

    /**
     * @return a representation of this BatchID as if the sizes are (quasi) circularly left shifted.
     * E.g.:
     * 1 4 3 1
     * 1 5 3 2
     * becomes
     * 1 4 3 1
     * 5 3 2 0
     * <p>
     * The first representation (accessible via iterator) provides offset and size for every step.
     * The second one represent how many records every step generator.
     * In the example, the first step produced 5 records, the fourth record in that batch of five
     * records (second step) generated 3 records, and so on...
     * The last step, does not produce any record (0 indeed).
     */
    public BatchID getShiftedRepresentation() {
        consolidate();

        BatchID shifted = new BatchID();

        shifted.offsets.addAll(offsets);
        shifted.sizes.addAll(sizes.subList(1, sizes.size()));
        shifted.sizes.add(0);
        return shifted;
    }

    /**
     * Use it only for testing
     */
    public BatchID clone() {
        BatchID cloned = new BatchID();
        cloned.sizes = new LinkedList<>(this.sizes);
        cloned.offsets = new LinkedList<>(this.offsets);
        cloned.newOffset = newOffset;
        cloned.newSize = newSize;
        return cloned;
    }

    @Override
    public String toString() {
        StringBuilder representation = new StringBuilder();

        for (Tuple2<Integer, Integer> couple : this) {
            if (representation.length() > 0) {
                representation.append(" - ");
            }
            representation.append(couple);
        }

        return representation.toString();
    }

    public String getDottedRepresentation() {
        String representation = String.join(".",
                offsets.stream()
                        .map(Object::toString)
                        .collect(Collectors.toList()));
        if (newOffset > 0) {
            representation += "." + newOffset;
        }
        return representation;
    }

    public long getTid() {
        return offsets.get(0);
    }

    @Override
    public int compareTo(BatchID other) {
        int thisLength = this.getNumberOfSteps();
        int otherLength = other.getNumberOfSteps();

        if (thisLength != otherLength) {
            throw new IllegalArgumentException("Cannot compare batch ids with different lengths: " +
                    thisLength + " != " + otherLength);
        }

        Iterator<Tuple2<Integer, Integer>> thisIterator = iterator();
        Iterator<Tuple2<Integer, Integer>> otherIterator = other.iterator();

        while (thisIterator.hasNext()) {
            Tuple2<Integer, Integer> thisCouple = thisIterator.next();
            Tuple2<Integer, Integer> otherCouple = otherIterator.next();

            if (!Objects.equals(thisCouple.f1, otherCouple.f1)) {
                throw new IllegalArgumentException("Cannot compare ids from different batches: " +
                        this.toString() + " <<>> " + other.toString());
            }

            if (thisCouple.f0 < otherCouple.f0) {
                return -1;
            }

            if (thisCouple.f0 > otherCouple.f0) {
                return 1;
            }
        }

        return 0;
    }
}
