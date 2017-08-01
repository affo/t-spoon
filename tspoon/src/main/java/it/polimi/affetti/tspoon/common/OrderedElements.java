package it.polimi.affetti.tspoon.common;

import java.io.Serializable;
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
public class OrderedElements<E> implements Iterable<E>, Serializable {
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
     * Removes and returns the first element if smaller (-1), bigger(1)
     * or equal (0) to {@code other}. The method returns {@code null} otherwise.
     *
     * @param threshold the element to which the first element is compared
     * @param lge       less (-1), greater (1) or equal (0)
     * @return The first element if {@code other} is bigger than the first
     * element of the list. {@code null} otherwise.
     * @throws IndexOutOfBoundsException if the list is empty
     */
    public E pollFirstConditionally(E threshold, int lge) {
        E e = null;

        if (comparator.compare(threshold, orderedElements.get(0)) == lge) {
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

    public boolean isEmpty() {
        return orderedElements.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return orderedElements.iterator();
    }

    public boolean remove(E equal) {
        return orderedElements.removeIf(equal::equals);
    }

    public <T> boolean remove(T equal, Function<E, T> keyExtractor) {
        return orderedElements.removeIf(e -> equal.equals(keyExtractor.apply(e)));
    }
}
