package it.polimi.affetti.tspoon.common;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.junit.Assert.*;

public class OrderedElementsTest {
    private OrderedElements<Integer> orderedElements;

    @Before
    public void setUp() {
        orderedElements = new OrderedElements<>(Comparator.comparingInt(i -> i));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testEmpty() {
        orderedElements.pollFirstConditionally(42, -1);
    }

    @Test
    public void testPolling() {
        orderedElements.addInOrder(0);
        orderedElements.addInOrder(1);
        orderedElements.addInOrder(2);
        orderedElements.addInOrder(3);
        orderedElements.addInOrder(4);
        orderedElements.addInOrder(5);

        int threshold = 3;

        assertEquals(0, orderedElements.pollFirstConditionally(threshold, 1).intValue());
        assertEquals(1, orderedElements.pollFirstConditionally(threshold, 1).intValue());
        assertEquals(2, orderedElements.pollFirstConditionally(threshold, 1).intValue());
        assertEquals(3, orderedElements.pollFirstConditionally(threshold, 0).intValue());
        assertNull(orderedElements.pollFirstConditionally(threshold, 1));
        assertEquals(orderedElements.size(), 2);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testOrder() throws IndexOutOfBoundsException {
        int[] input = new int[1000];
        for (int i = 0; i < input.length; i++) {
            input[i] = ThreadLocalRandom.current().nextInt(-50, 50);
            orderedElements.addInOrder(input[i]);
        }

        Arrays.sort(input);

        for (int i = 0; i < input.length; i++) {
            assertEquals(input[i], orderedElements.pollFirstConditionally(100, 1).intValue());
        }

        // the list should now be empty
        thrown.expect(IndexOutOfBoundsException.class);
        orderedElements.pollFirstConditionally(42, -1);
    }

    @Test
    public void testGap() {
        orderedElements.addInOrder(18);
        orderedElements.addInOrder(50);
        orderedElements.addInOrder(20);
        orderedElements.addInOrder(21);
        orderedElements.addInOrder(19);
        orderedElements.addInOrder(30);
        orderedElements.addInOrder(24);

        int threshold = 18;

        assertEquals(18, orderedElements.pollFirstConditionally(threshold, 0).intValue());
        threshold++;
        assertEquals(19, orderedElements.pollFirstConditionally(threshold, 0).intValue());
        threshold++;
        assertEquals(20, orderedElements.pollFirstConditionally(threshold, 0).intValue());
        threshold++;
        assertEquals(21, orderedElements.pollFirstConditionally(threshold, 0).intValue());
        threshold++;
        assertNull(orderedElements.pollFirstConditionally(threshold, 0));

        assertEquals(24, orderedElements.pollFirstConditionally(0, -1).intValue());
        assertEquals(30, orderedElements.pollFirstConditionally(0, -1).intValue());
        assertEquals(50, orderedElements.pollFirstConditionally(0, -1).intValue());
    }

    @Test
    public void testRemove() {
        orderedElements.addInOrder(0);
        orderedElements.addInOrder(1);
        orderedElements.addInOrder(2);
        orderedElements.addInOrder(3);
        orderedElements.addInOrder(4);
        orderedElements.addInOrder(5);

        assertTrue(orderedElements.remove(2));
        assertEquals(5, orderedElements.size());

        assertTrue(orderedElements.remove(4));
        assertEquals(4, orderedElements.size());

        assertFalse(orderedElements.remove(42));
        assertEquals(4, orderedElements.size());
    }
}
