package it.polimi.affetti.tspoon.common;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
        List<Long> input = LongStream.range(0, 1000).boxed().collect(Collectors.toList());
        Collections.shuffle(input);
        for (long i = 0; i < 1000; i++) {
            timestamps.addInOrder(i);
        }

        Collections.sort(input);
        assertEquals(input, timestamps.getContiguousElements());
    }

    @Test
    public void testGap() {
        timestamps.addInOrder(18L);
        timestamps.addInOrder(50L);
        timestamps.addInOrder(20L);
        timestamps.addInOrder(21L);
        timestamps.addInOrder(19L);
        timestamps.addInOrder(51L);
        timestamps.addInOrder(52L);

        assertEquals(Arrays.asList(18L, 19L, 20L, 21L), timestamps.removeContiguous());
        assertEquals(3, timestamps.size());
        assertEquals(Arrays.asList(50L, 51L, 52L), timestamps.removeContiguous());
        assertTrue(timestamps.isEmpty());
    }

    @Test
    public void testContiguousWith() throws IndexOutOfBoundsException {
        timestamps.addInOrder(18L);
        timestamps.addInOrder(50L);
        timestamps.addInOrder(20L);
        timestamps.addInOrder(21L);
        timestamps.addInOrder(19L);
        timestamps.addInOrder(51L);
        timestamps.addInOrder(52L);

        assertEquals(Collections.emptyList(), timestamps.removeContiguousWith(0));
        assertEquals(7, timestamps.size());
        assertEquals(Collections.emptyList(), timestamps.removeContiguousWith(16));
        assertEquals(7, timestamps.size());
        assertEquals(Arrays.asList(18L, 19L, 20L, 21L), timestamps.removeContiguousWith(17));
        assertEquals(3, timestamps.size());
        assertEquals(Collections.emptyList(), timestamps.removeContiguousWith(42));
        assertEquals(3, timestamps.size());
        assertEquals(Arrays.asList(50L, 51L, 52L), timestamps.removeContiguousWith(49));
        assertTrue(timestamps.isEmpty());
    }

    @Test
    public void testNoRepetitions() {
        assertEquals(0, timestamps.size());
        timestamps.addInOrderWithoutRepetition(42L);
        assertEquals(1, timestamps.size());
        timestamps.addInOrderWithoutRepetition(42L);
        assertEquals(1, timestamps.size());
        timestamps.addInOrderWithoutRepetition(43L);
        assertEquals(2, timestamps.size());
        timestamps.addInOrderWithoutRepetition(43L);
        assertEquals(2, timestamps.size());
    }

    @Test
    public void testNoRepetitionsBig() {
        assertEquals(0, timestamps.size());

        for (long i = 0; i < 1000; i++) {
            timestamps.addInOrderWithoutRepetition(i);
        }

        assertEquals(1000, timestamps.size());

        for (long i = 0; i < 1000; i++) {
            timestamps.addInOrderWithoutRepetition(i);
        }

        assertEquals(1000, timestamps.size());
    }
}
