package it.polimi.affetti.tspoon.tgraph.twopc;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 03/08/17.
 */
public class SequencerTest {
    private StrictnessEnforcer.Sequencer sequencer;

    @Before
    public void setUp() {
        sequencer = new StrictnessEnforcer.Sequencer();
    }

    @Test
    public void monotonicSeries() {
        final int numberOfTransactions = 1000;
        for (int i = 1; i <= numberOfTransactions; i++) {
            sequencer.addTransaction(i);
        }

        int i = 0;
        for (long actual : sequencer.nextAvailableSequence()) {
            assertEquals(i + 1, actual);
            i++;
        }

        assertEquals(numberOfTransactions, i);
        assertEquals(Collections.emptyList(), sequencer.nextAvailableSequence());
    }

    @Test
    public void waitingForSomething() {

        sequencer.addTransaction(1);
        sequencer.addTransaction(2);
        sequencer.signalReplay(4);
        assertEquals(Arrays.asList(1L, 2L), sequencer.nextAvailableSequence());
        assertEquals(Collections.emptyList(), sequencer.nextAvailableSequence()); // empty...

        sequencer.addTransaction(5);
        assertEquals(Collections.emptyList(), sequencer.nextAvailableSequence()); // 5 is not contiguous! Still waiting for 3

        sequencer.addTransaction(4);
        assertEquals(Collections.emptyList(), sequencer.nextAvailableSequence()); // 4 is not contiguous! Still waiting for 3

        sequencer.addTransaction(3);
        assertEquals(Arrays.asList(3L, 4L, 5L), sequencer.nextAvailableSequence()); // contiguous with 2, but still waiting for 4

        assertEquals(Collections.emptyList(), sequencer.nextAvailableSequence()); // empty...
    }
}
