package it.polimi.affetti.tspoon.common;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

public class OrderedElementsTest extends TestCase {
    private OrderedElements<Integer> orderedElements;

    public void setUp() {
        orderedElements = new OrderedElements<>(Comparator.comparingInt(i -> i));
    }

    public void testEmpty() {
        assertNull(orderedElements.pollFirstConditionally(42));
    }

    public void testPolling() {
        orderedElements.addInOrder(0);
        orderedElements.addInOrder(1);
        orderedElements.addInOrder(2);
        orderedElements.addInOrder(3);
        orderedElements.addInOrder(4);
        orderedElements.addInOrder(5);

        int threshold = 3;

        assertEquals(0, orderedElements.pollFirstConditionally(threshold).intValue());
        assertEquals(1, orderedElements.pollFirstConditionally(threshold).intValue());
        assertEquals(2, orderedElements.pollFirstConditionally(threshold).intValue());
        assertEquals(3, orderedElements.pollFirstConditionally(threshold).intValue());
        assertNull(orderedElements.pollFirstConditionally(threshold));
        assertEquals(orderedElements.size(), 2);
    }

    public void testOrder() {
        int[] input = new int[1000];
        for (int i = 0; i < input.length; i++) {
            input[i] = ThreadLocalRandom.current().nextInt(-50, 50);
            orderedElements.addInOrder(input[i]);
        }

        Arrays.sort(input);

        for (int i = 0; i < input.length; i++) {
            assertEquals(input[i], orderedElements.pollFirstConditionally(100).intValue());
        }

        // the list should now be empty
        assertNull(orderedElements.pollFirstConditionally(42));
    }
}
