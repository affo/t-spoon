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

    @Override
    public synchronized void addInOrder(Integer element) {
        super.addInOrder(element);
    }

    private List<Integer> operateOnContiguous(Integer start, int threshold, boolean remove) {
        List<Integer> result = new LinkedList<>();

        if (!isEmpty()) {
            ListIterator<Integer> it = iterator();

            while (it.hasNext()) {
                int current = it.next();
                if (start != null && start + 1 != current) {
                    // gapDetected
                    break;
                }
                start = current;

                if (start < threshold) {
                    result.add(start);

                    if (remove) {
                        it.remove();
                    }
                }
            }
        }

        return result;
    }

    public synchronized List<Integer> getContiguousElements() {
        return operateOnContiguous(null, Integer.MAX_VALUE, false);
    }

    public synchronized List<Integer> removeContiguous(int threshold) {
        return operateOnContiguous(null, threshold, true);
    }

    public synchronized List<Integer> removeContiguous() {
        return operateOnContiguous(null, Integer.MAX_VALUE, true);
    }

    public synchronized List<Integer> removeContiguousWith(int timestamp) {
        return operateOnContiguous(timestamp, Integer.MAX_VALUE, true);
    }

    @Override
    public synchronized String toString() {
        return super.toString();
    }
}
