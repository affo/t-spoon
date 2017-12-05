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
    private TwoPCRuntimeContext twoPCRuntimeContext;
    private AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;
    private AbstractStateOperatorTransactionCloser stateOperatorTransactionCloser;
    private CloseSinkTransactionCloser sinkTransactionCloser;

    @Before
    public void setUp() throws Exception {
        if (isDurable()) {
            TransactionEnvironment.get().setDurable(true);
        } else {
            TransactionEnvironment.get().setDurable(false);
        }

        twoPCRuntimeContext = TransactionEnvironment.get().getTwoPCRuntimeContext();
        openOperatorTransactionCloser = twoPCRuntimeContext.getSourceTransactionCloser();
        openOperatorTransactionCloser.open();
        stateOperatorTransactionCloser = twoPCRuntimeContext.getAtStateTransactionCloser();
        stateOperatorTransactionCloser.open();
        sinkTransactionCloser = twoPCRuntimeContext.getSinkTransactionCloser();
        sinkTransactionCloser.open();
    }

    protected abstract boolean isDurable();

    @After
    public void tearDown() throws Exception {
        openOperatorTransactionCloser.close();
        stateOperatorTransactionCloser.close();
        sinkTransactionCloser.close();
    }

    private boolean noMoreMessagesInQueue(WithMessageQueue<?> withMessageQueue) throws InterruptedException {
        Object receive = withMessageQueue.receive();
        return receive == null;
    }

    @Test
    public void simpleTest() throws Exception {
        AtOpenListener openListener = new AtOpenListener();
        Address coordinatorAddress = openOperatorTransactionCloser.getServerAddress();
        AtStateListener stateListener = new AtStateListener(coordinatorAddress);
        openOperatorTransactionCloser.subscribeTo(1, openListener);
        openOperatorTransactionCloser.subscribeTo(2, openListener);
        openOperatorTransactionCloser.subscribeTo(3, openListener);
        stateOperatorTransactionCloser.subscribeTo(1, stateListener);
        stateOperatorTransactionCloser.subscribeTo(2, stateListener);
        stateOperatorTransactionCloser.subscribeTo(3, stateListener);

        openListener.setVerbose();
        stateListener.setVerbose();

        Metadata[] metas = {
                new Metadata(1),
                new Metadata(2),
                new Metadata(3)
        };

        for (int i = 0; i < metas.length; i++) {
            metas[i].coordinator = coordinatorAddress;
            metas[i].addCohort(stateOperatorTransactionCloser.getServerAddress());
        }

        for (int i = 0; i < metas.length; i++) {
            sinkTransactionCloser.onMetadata(metas[i]);
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

    @Test
    public void multipleStateListeners() throws Exception {
        AtOpenListener openListener = new AtOpenListener();
        Address coordinatorAddress = openOperatorTransactionCloser.getServerAddress();

        AtStateListener[] stateListeners = new AtStateListener[10];
        for (int i = 0; i < stateListeners.length; i++) {
            stateListeners[i] = new AtStateListener(coordinatorAddress);
            stateListeners[i].setVerbose();
        }


        final int numberOfTransactions = 10;
        Metadata[] metas = new Metadata[stateListeners.length * numberOfTransactions];
        // listener 0 is subscribed from 1 to 10
        // listener 1 is subscribed from 11 to 20
        // ...
        // listener 9 is subscribed from 91 to 100
        for (int i = 0; i < stateListeners.length; i++) {
            for (int j = 0; j < numberOfTransactions; j++) {
                int index = i + j * numberOfTransactions;
                stateOperatorTransactionCloser.subscribeTo(index + 1, stateListeners[i]);
                // the open is interested in everything
                openOperatorTransactionCloser.subscribeTo(index + 1, openListener);
                metas[index] = new Metadata(index + 1);
                metas[index].coordinator = coordinatorAddress;
                metas[index].addCohort(stateOperatorTransactionCloser.getServerAddress());
            }
        }

        openListener.setVerbose();

        // emits stuff
        for (int i = 0; i < metas.length; i++) {
            sinkTransactionCloser.onMetadata(metas[i]);
        }

        for (int i = 0; i < metas.length; i++) {
            CloseTransactionNotification msg = openListener.receive();
            assertEquals((long) i + 1, msg.timestamp);
        }

        assertTrue(noMoreMessagesInQueue(openListener));


        for (int i = 0; i < stateListeners.length; i++) {
            AtStateListener listener = stateListeners[i];
            for (int j = 0; j < numberOfTransactions; j++) {
                CloseTransactionNotification msg = listener.receive();
                assertEquals((long) i + j * numberOfTransactions + 1, msg.timestamp);
            }
            assertTrue(noMoreMessagesInQueue(listener));
        }
    }

    @Test
    public void multipleStateListenersOnSameKey() throws Exception {
        AtOpenListener openListener = new AtOpenListener();
        Address coordinatorAddress = openOperatorTransactionCloser.getServerAddress();

        AtStateListener[] stateListeners = new AtStateListener[10];
        for (int i = 0; i < stateListeners.length; i++) {
            stateListeners[i] = new AtStateListener(coordinatorAddress);
            stateListeners[i].setVerbose();
        }

        final int numberOfTransactions = 10;
        Metadata[] metas = new Metadata[numberOfTransactions];
        for (int i = 0; i < numberOfTransactions; i++) {
            metas[i] = new Metadata(i + 1);
            metas[i].coordinator = coordinatorAddress;
            metas[i].addCohort(stateOperatorTransactionCloser.getServerAddress());
            // the open is interested in everything
            openOperatorTransactionCloser.subscribeTo(i + 1, openListener);
        }

        // every listener is subscribed to every transaction
        for (int i = 0; i < stateListeners.length; i++) {
            for (int j = 0; j < numberOfTransactions; j++) {
                stateOperatorTransactionCloser.subscribeTo(j + 1, stateListeners[i]);
            }
        }

        openListener.setVerbose();

        // emits stuff
        for (int i = 0; i < metas.length; i++) {
            sinkTransactionCloser.onMetadata(metas[i]);
        }

        for (int i = 0; i < metas.length; i++) {
            CloseTransactionNotification msg = openListener.receive();
            assertEquals((long) i + 1, msg.timestamp);
        }

        assertTrue(noMoreMessagesInQueue(openListener));

        for (int i = 0; i < stateListeners.length; i++) {
            AtStateListener listener = stateListeners[i];
            for (int j = 0; j < numberOfTransactions; j++) {
                CloseTransactionNotification msg = listener.receive();
                try {
                    assertEquals((long) j + 1, msg.timestamp);
                } catch (AssertionError e) {
                    System.out.println("azz");
                }
            }
            assertTrue(noMoreMessagesInQueue(listener));
        }
    }
}
