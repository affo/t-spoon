package it.polimi.affetti.tspoon.common;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by affo on 04/08/17.
 */
public class OrderedTimestamps extends OrderedElements<Integer> {

    public OrderedTimestamps() {
        super(Comparator.comparingInt(i -> i));
    }

    private List<Integer> operateOnContiguous(int threshold, boolean remove) {
        List<Integer> result = new LinkedList<>();

        if (!isEmpty()) {
            ListIterator<Integer> it = iterator();
            Integer previous = null;

            while (it.hasNext()) {
                int current = it.next();
                if (previous != null && previous + 1 != current) {
                    // gapDetected
                    break;
                }
                previous = current;

                if (previous < threshold) {
                    result.add(previous);

                    if (remove) {
                        it.remove();
                    }
                }
            }
        }

        return result;
    }

    public List<Integer> getContiguousElements() {
        return operateOnContiguous(Integer.MAX_VALUE, false);
    }

    public List<Integer> removeContiguous(int threshold) {
        return operateOnContiguous(threshold, true);
    }

    public List<Integer> removeContiguous() {
        return operateOnContiguous(Integer.MAX_VALUE, true);
    }
}
