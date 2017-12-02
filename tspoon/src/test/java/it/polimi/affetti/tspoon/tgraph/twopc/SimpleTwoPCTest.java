package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by affo on 02/12/17.
 */
public abstract class SimpleTwoPCTest {
    private CoordinatorTransactionCloser coordinator;
    private StateOperatorTransactionCloser stateOp;
    private CloseSinkTransactionCloser sink;

    @Before
    public void setUp() throws Exception {
        if (isDurable()) {
            TransactionEnvironment.get().setDurable(true);
        }

        TwoPCFactory factory = new OptimisticTwoPCFactory(); // doesn't matter actually
        coordinator = factory.getSourceTransactionCloser();
        coordinator.open();
        stateOp = factory.getAtStateTransactionCloser();
        stateOp.open();
        sink = factory.getSinkTransactionCloser();
        sink.open();
    }

    protected abstract boolean isDurable();

    @After
    public void tearDown() throws Exception {
        coordinator.close();
        stateOp.close();
        sink.close();
    }

    private boolean noMoreMessagesInQueue(WithMessageQueue<?> withMessageQueue) throws InterruptedException {
        Object receive = withMessageQueue.receive();
        return receive == null;
    }

    @Test
    public void simpleTest() throws Exception {
        AtOpenListener openListener = new AtOpenListener();
        Address coordinatorAddress = coordinator.getOpenServerAddress();
        AtStateListener stateListener = new AtStateListener(coordinatorAddress);
        coordinator.subscribe(openListener);
        stateOp.subscribe(stateListener);

        openListener.setVerbose();
        stateListener.setVerbose();

        Metadata[] metas = {
                new Metadata(1),
                new Metadata(2),
                new Metadata(3)
        };

        for (int i = 0; i < metas.length; i++) {
            metas[i].coordinator = coordinatorAddress;
            metas[i].addCohort(stateOp.getStateServerAddress());
        }

        for (int i = 0; i < metas.length; i++) {
            sink.onMetadata(metas[i]);
        }

        for (int i = 0; i < metas.length; i++) {
            CloseTransactionNotification msg = openListener.receive();
            assertEquals((long) i + 1, msg.timestamp);
        }

        for (int i = 0; i < metas.length; i++) {
            CloseTransactionNotification msg = stateListener.receive();
            assertEquals((long) i + 1, msg.timestamp);
        }

        assertTrue(noMoreMessagesInQueue(openListener));
        assertTrue(noMoreMessagesInQueue(stateListener));
    }
}
