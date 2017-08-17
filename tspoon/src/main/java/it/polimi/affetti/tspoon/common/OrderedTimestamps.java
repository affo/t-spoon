package it.polimi.affetti.tspoon.common;

/**
 * Created by affo on 04/08/17.
 */
public class OrderedTimestamps extends OrderedElements<Integer> {
    public OrderedTimestamps() {
        super(Integer::longValue);
    }
}
