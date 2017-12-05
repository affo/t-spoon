package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 * Created by affo on 05/12/17.
 */
public class TransactionClosersTest {
    private AbstractOpenOperatorTransactionCloser coordinator;
    private AbstractStateOperatorTransactionCloser stateOp;

    @Before
    public void setUp() throws Exception {
        TwoPCRuntimeContext twoPCRuntimeContext = TransactionEnvironment.get().getTwoPCRuntimeContext();
        coordinator = twoPCRuntimeContext.getSourceTransactionCloser();
        stateOp = twoPCRuntimeContext.getAtStateTransactionCloser();
    }

    @Test
    public void singletonTest() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);

        final Object monitor = new Object();
        final int limit = 100;
        final int[] count = new int[1];

        for (int i = 0; i < limit; i++) {
            pool.submit(() -> {
                TwoPCRuntimeContext runtimeContext = TransactionEnvironment.get().getTwoPCRuntimeContext();
                assertTrue(runtimeContext.getSourceTransactionCloser() == coordinator);
                assertTrue(runtimeContext.getAtStateTransactionCloser() == stateOp);
                synchronized (monitor) {
                    count[0]++;
                    monitor.notifyAll();
                }
            });
        }

        synchronized (monitor) {
            while (count[0] < 100) {
                monitor.wait();
            }
            pool.shutdown();
        }
    }
}
