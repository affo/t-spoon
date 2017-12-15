package it.polimi.affetti.tspoon.tgraph.twopc;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 03/08/17.
 */
public class CompleteBatchCheckerTest {
    private StrictnessEnforcer.CompleteBatcher completeBatchChecker;

    @Before
    public void setUp() {
        completeBatchChecker = new StrictnessEnforcer.CompleteBatcher();
    }

    @Test
    public void monotonicSeries() {
        final int numberOfTransactions = 1000;
        for (int i = 1; i <= numberOfTransactions; i++) {
            completeBatchChecker.addTransaction(i);
        }

        int i = 0;
        for (int actual : completeBatchChecker.nextBatch()) {
            assertEquals(i + 1, actual);
            i++;
        }

        assertEquals(numberOfTransactions, i);
        assertEquals(Collections.emptyList(), completeBatchChecker.nextBatch());
    }

    @Test
    public void waitingForSomething() {

        completeBatchChecker.addTransaction(1);
        completeBatchChecker.addTransaction(2);
        completeBatchChecker.signalReplay(4);
        assertEquals(Arrays.asList(1, 2), completeBatchChecker.nextBatch());
        assertEquals(Collections.emptyList(), completeBatchChecker.nextBatch()); // empty...

        completeBatchChecker.addTransaction(5);
        assertEquals(Collections.emptyList(), completeBatchChecker.nextBatch()); // 5 is not contiguous! Still waiting for 3

        completeBatchChecker.addTransaction(4);
        assertEquals(Collections.emptyList(), completeBatchChecker.nextBatch()); // 4 is not contiguous! Still waiting for 3

        completeBatchChecker.addTransaction(3);
        assertEquals(Arrays.asList(3, 4, 5), completeBatchChecker.nextBatch()); // contiguous with 2, but still waiting for 4

        assertEquals(Collections.emptyList(), completeBatchChecker.nextBatch()); // empty...
    }
}
