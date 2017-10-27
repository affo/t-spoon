package it.polimi.affetti.tspoon.common;

import java.util.ListIterator;

/**
 * Created by affo on 04/08/17.
 */
public class OrderedTimestamps extends OrderedElements<Integer> {
    public OrderedTimestamps() {
        super(Integer::longValue);
    }

    public void addInOrderWithoutRepetition(Integer timestamp) {
        ListIterator<Integer> it = iterator();

        boolean added = false;

        while (it.hasNext() && !added) {
            Integer nextTimestamp = it.next();
            if (nextTimestamp >= timestamp) {
                if (nextTimestamp > timestamp) {
                    it.previous();
                    it.add(timestamp);
                }
                added = true;
            }
        }

        // add in tail
        if (!added) {
            it.add(timestamp);
        }
    }
}
