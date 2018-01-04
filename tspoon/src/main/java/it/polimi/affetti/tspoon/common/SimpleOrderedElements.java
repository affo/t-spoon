package it.polimi.affetti.tspoon.common;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;

public class SimpleOrderedElements<E extends Comparable<E>> implements Iterable<E>, Serializable {
    private List<E> orderedElements;

    public SimpleOrderedElements() {
        this.orderedElements = new LinkedList<>();
    }

    /**
     * Adds {@code element} in order using their natural ordering.
     *
     * @param element the element to add
     */
    public void addInOrder(E element) {
        ListIterator<E> it = orderedElements.listIterator();
        boolean added = false;

        while (it.hasNext() && !added) {
            if (it.next().compareTo(element) > 0) {
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
     * Removes and returns the first element if the condition is true.
     * The method returns {@code null} otherwise.
     *
     * @throws IndexOutOfBoundsException if the list is empty
     */
    public E pollFirstConditionally(Predicate<E> condition) {
        E e = null;

        if (condition.test(orderedElements.get(0))) {
            e = orderedElements.remove(0);
        }

        return e;
    }

    @Override
    public Iterator<E> iterator() {
        return orderedElements.iterator();
    }

    public int size() {
        return orderedElements.size();
    }

    public List<E> toList() {
        return toList(false);
    }

    public List<E> toList(boolean copy) {
        List<E> result;

        if (copy) {
            result = new ArrayList<>(size());
            result.addAll(orderedElements);
        } else {
            result = orderedElements;
        }

        return result;
    }
}
