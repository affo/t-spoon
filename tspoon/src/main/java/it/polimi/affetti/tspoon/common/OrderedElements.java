package it.polimi.affetti.tspoon.common;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;

/**
 * This class represents a sequence of elements whose order is specified
 * by a TimestampExtractor, i.e. a function from E to Long.
 * It offers methods to {@link OrderedElements#addInOrder(Object) add in order}
 * and to {@link OrderedElements#pollFirstConditionally remove the first element of the sequence}
 * if smaller than a given one. It also offers methods to get and remove contiguous elements (by timestamp).
 * <p>
 * {@code OrderedElements} gives a linear time complexity (with the size of the sequence) on
 * adding in order and a constant complexity on removing the first element.
 * <p>
 * This class is thread-safe.
 */
public class OrderedElements<E> implements Iterable<E>, Serializable {
    private List<E> orderedElements;
    private TimestampExtractor<E> timestampExtractor;

    public OrderedElements(TimestampExtractor<E> timestampExtractor) {
        this.orderedElements = new LinkedList<>();
        this.timestampExtractor = timestampExtractor;
    }

    /**
     * Adds {@code element} in order using the {@code Comparator} provided
     * by constructor.
     *
     * @param element the element to add
     */
    public synchronized void addInOrder(E element) {
        long timestamp = timestampExtractor.apply(element);
        ListIterator<E> it = orderedElements.listIterator();
        boolean added = false;

        while (it.hasNext() && !added) {
            long nextTimestamp = timestampExtractor.apply(it.next());
            if (nextTimestamp >= timestamp) {
                it.previous();
                it.add(element);
                added = true;
            }
        }

        // add in tail
        if (!added) {
            it.add(element);
        }
    }

    /**
     * Removes and returns the first element if smaller (-1), bigger(1)
     * or equal (0) to {@code other}. The method returns {@code null} otherwise.
     *
     * @param threshold the element to which the first element is compared
     * @param lge       less (-1), greater (1) or equal (0)
     * @return The first element if {@code other} is bigger than the first
     * element of the list. {@code null} otherwise.
     * @throws IndexOutOfBoundsException if the list is empty
     */
    public synchronized E pollFirstConditionally(long threshold, int lge) {
        E e = null;

        if (Long.compare(threshold, timestampExtractor.apply(orderedElements.get(0))) == lge) {
            e = orderedElements.remove(0);
        }

        return e;
    }

    /**
     * Threshold not included
     *
     * @param startTimestamp
     * @param threshold
     * @param remove
     * @return
     */
    private List<E> operateOnContiguous(Long startTimestamp, long threshold, boolean remove) {
        List<E> result = new LinkedList<>();

        if (!isEmpty()) {
            ListIterator<E> it = iterator();

            while (it.hasNext()) {
                E element = it.next();
                long currentTimestamp = timestampExtractor.apply(element);
                if (currentTimestamp >= threshold ||
                        startTimestamp != null && startTimestamp + 1 != currentTimestamp) {
                    // gapDetected or above threshold, exit
                    break;
                }
                startTimestamp = currentTimestamp;

                result.add(element);

                if (remove) {
                    it.remove();
                }
            }
        }

        return result;
    }

    public synchronized List<E> getContiguousElements() {
        return operateOnContiguous(null, Long.MAX_VALUE, false);
    }

    public synchronized List<E> removeContiguous(int threshold) {
        return operateOnContiguous(null, threshold, true);
    }

    public synchronized List<E> removeContiguous() {
        return operateOnContiguous(null, Long.MAX_VALUE, true);
    }

    public synchronized List<E> removeContiguousWith(long timestamp) {
        return operateOnContiguous(timestamp, Long.MAX_VALUE, true);
    }

    public synchronized List<E> removeContiguousWith(long timestamp, int threshold) {
        return operateOnContiguous(timestamp, threshold, true);
    }

    public synchronized List<E> getContiguousWith(long timestamp) {
        return operateOnContiguous(timestamp, Long.MAX_VALUE, false);
    }

    /**
     * @return The size of the list
     */
    public int size() {
        return orderedElements.size();
    }

    public boolean isEmpty() {
        return orderedElements.isEmpty();
    }

    @Override
    public ListIterator<E> iterator() {
        return orderedElements.listIterator();
    }

    public synchronized boolean remove(E equal) {
        return orderedElements.removeIf(equal::equals);
    }

    public synchronized <T> boolean remove(T equal, Function<E, T> keyExtractor) {
        return orderedElements.removeIf(e -> equal.equals(keyExtractor.apply(e)));
    }

    @Override
    public synchronized String toString() {
        return orderedElements.toString();
    }

    public synchronized E getFirst() {
        E first = null;

        if (!orderedElements.isEmpty()) {
            first = orderedElements.get(0);
        }

        return first;
    }

    public interface TimestampExtractor<E> extends Function<E, Long>, Serializable {
    }
}
