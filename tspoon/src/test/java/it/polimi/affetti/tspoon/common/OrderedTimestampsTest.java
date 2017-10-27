package it.polimi.affetti.tspoon.common;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OrderedTimestampsTest {
    private OrderedTimestamps timestamps;

    @Before
    public void setUp() {
        timestamps = new OrderedTimestamps();
    }

    @Test
    public void testContiguous() throws IndexOutOfBoundsException {
        List<Integer> input = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        Collections.shuffle(input);
        for (int i = 0; i < 1000; i++) {
            timestamps.addInOrder(i);
        }

        Collections.sort(input);
        assertEquals(input, timestamps.getContiguousElements());
    }

    @Test
    public void testGap() {
        timestamps.addInOrder(18);
        timestamps.addInOrder(50);
        timestamps.addInOrder(20);
        timestamps.addInOrder(21);
        timestamps.addInOrder(19);
        timestamps.addInOrder(51);
        timestamps.addInOrder(52);

        assertEquals(Arrays.asList(18, 19, 20, 21), timestamps.removeContiguous());
        assertEquals(3, timestamps.size());
        assertEquals(Arrays.asList(50, 51, 52), timestamps.removeContiguous());
        assertTrue(timestamps.isEmpty());
    }

    @Test
    public void testContiguousWith() throws IndexOutOfBoundsException {
        timestamps.addInOrder(18);
        timestamps.addInOrder(50);
        timestamps.addInOrder(20);
        timestamps.addInOrder(21);
        timestamps.addInOrder(19);
        timestamps.addInOrder(51);
        timestamps.addInOrder(52);

        assertEquals(Collections.emptyList(), timestamps.removeContiguousWith(0));
        assertEquals(7, timestamps.size());
        assertEquals(Collections.emptyList(), timestamps.removeContiguousWith(16));
        assertEquals(7, timestamps.size());
        assertEquals(Arrays.asList(18, 19, 20, 21), timestamps.removeContiguousWith(17));
        assertEquals(3, timestamps.size());
        assertEquals(Collections.emptyList(), timestamps.removeContiguousWith(42));
        assertEquals(3, timestamps.size());
        assertEquals(Arrays.asList(50, 51, 52), timestamps.removeContiguousWith(49));
        assertTrue(timestamps.isEmpty());
    }

    @Test
    public void testNoRepetitions() {
        assertEquals(0, timestamps.size());
        timestamps.addInOrderWithoutRepetition(42);
        assertEquals(1, timestamps.size());
        timestamps.addInOrderWithoutRepetition(42);
        assertEquals(1, timestamps.size());
        timestamps.addInOrderWithoutRepetition(43);
        assertEquals(2, timestamps.size());
        timestamps.addInOrderWithoutRepetition(43);
        assertEquals(2, timestamps.size());
    }

    @Test
    public void testNoRepetitionsBig() {
        assertEquals(0, timestamps.size());

        for (int i = 0; i < 1000; i++) {
            timestamps.addInOrderWithoutRepetition(i);
        }

        assertEquals(1000, timestamps.size());

        for (int i = 0; i < 1000; i++) {
            timestamps.addInOrderWithoutRepetition(i);
        }

        assertEquals(1000, timestamps.size());
    }
}
