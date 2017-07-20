package it.polimi.affetti.tspoon.common;

import java.util.*;
import java.util.function.Function;

/**
 * This class represents a sequence of elements whose order is specified
 * by a {@code Comparator}. It offers methods to {@link OrderedElements#addInOrder(Object) add in order}
 * and to {@link OrderedElements#pollFirstConditionally remove the first element of the sequence}
 * if smaller than a given one.
 * <p>
 * {@code OrderedElements} gives a linear time complexity (with the size of the sequence) on
 * adding in order and a constant complexity on removing the first element.
 */
public class OrderedElements<E> implements Iterable<E> {
    private List<E> orderedElements;
    private Comparator<E> comparator;

    public OrderedElements(Comparator<E> comparator) {
        orderedElements = new LinkedList<>();
        this.comparator = comparator;
    }

    /**
     * Adds {@code element} in order using the {@code Comparator} provided
     * by constructor.
     *
     * @param element the element to add
     */
    public void addInOrder(E element) {
        ListIterator<E> it = orderedElements.listIterator();
        boolean added = false;

        while (it.hasNext() && !added) {
            if (comparator.compare(it.next(), element) >= 0) {
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
     * Removes and returns the first element if bigger than {@code other}.
     * The method returns {@code null} otherwise and if the list is empty.
     *
     * @param other the element to which the first element is compared
     * @return The first element if {@code other} is bigger than the first
     * element of the list. {@code null} otherwise.
     */
    public E pollFirstConditionally(E other) {
        E e = null;

        if (!orderedElements.isEmpty() &&
                comparator.compare(other, orderedElements.get(0)) >= 0) {
            e = orderedElements.remove(0);
        }

        return e;
    }

    /**
     * @return The size of the list
     */
    public int size() {
        return orderedElements.size();
    }

    @Override
    public Iterator<E> iterator() {
        return orderedElements.iterator();
    }

    public <T> boolean remove(T equal, Function<E, T> keyExtractor) {
        return orderedElements.removeIf(e -> equal.equals(keyExtractor.apply(e)));
    }
}
