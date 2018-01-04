package it.polimi.affetti.tspoon.common;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SimpleOrderedElementsTest {
    @Test(expected = IndexOutOfBoundsException.class)
    public void testEmpty() {
        SimpleOrderedElements<Integer> orderedElements = new SimpleOrderedElements<>();
        orderedElements.pollFirstConditionally(i -> i < 42);
    }

    @Test
    public void testPolling() {
        SimpleOrderedElements<Integer> orderedElements = new SimpleOrderedElements<>();
        orderedElements.addInOrder(0);
        orderedElements.addInOrder(1);
        orderedElements.addInOrder(2);
        orderedElements.addInOrder(3);
        orderedElements.addInOrder(4);
        orderedElements.addInOrder(5);

        Predicate<Integer> condition = i -> i <= 3;

        assertEquals(0, orderedElements.pollFirstConditionally(condition).intValue());
        assertEquals(1, orderedElements.pollFirstConditionally(condition).intValue());
        assertEquals(2, orderedElements.pollFirstConditionally(condition).intValue());
        assertEquals(3, orderedElements.pollFirstConditionally(condition).intValue());
        assertNull(orderedElements.pollFirstConditionally(condition));
        assertEquals(orderedElements.size(), 2);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testOrder() throws IndexOutOfBoundsException {
        SimpleOrderedElements<Integer> orderedElements = new SimpleOrderedElements<>();
        int[] input = new int[1000];
        for (int i = 0; i < input.length; i++) {
            input[i] = ThreadLocalRandom.current().nextInt(-50, 50);
            orderedElements.addInOrder(input[i]);
        }

        Arrays.sort(input);

        for (int i = 0; i < input.length; i++) {
            assertEquals(input[i], orderedElements.pollFirstConditionally(j -> j <= 100).intValue());
        }

        // the list should now be empty
        thrown.expect(IndexOutOfBoundsException.class);
        orderedElements.pollFirstConditionally(j -> j <= 100);
    }
}
