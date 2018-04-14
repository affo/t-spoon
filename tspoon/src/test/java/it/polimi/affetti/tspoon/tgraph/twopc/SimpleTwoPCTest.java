package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by affo on 02/12/17.
 */
public abstract class SimpleTwoPCTest {
    private TRuntimeContext tRuntimeContext;
    private AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;
    private AbstractStateOperatorTransactionCloser stateOperatorTransactionCloser;
    private AbstractCloseOperatorTransactionCloser sinkTransactionCloser;

    @Before
    public void setUp() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureTransactionalEnvironment(TransactionEnvironment.get(env));
        
        tRuntimeContext = TransactionEnvironment.get(env).createTransactionalRuntimeContext();
        tRuntimeContext.resetTransactionClosers();
        // TODO develop a strategy for implementing tests for different subscription modes
        tRuntimeContext.setSubscriptionMode(AbstractTwoPCParticipant.SubscriptionMode.GENERIC);
        openOperatorTransactionCloser = tRuntimeContext.getSourceTransactionCloser(0);
        openOperatorTransactionCloser.open();
        stateOperatorTransactionCloser = tRuntimeContext.getAtStateTransactionCloser(0);
        stateOperatorTransactionCloser.open();
        sinkTransactionCloser = tRuntimeContext.getSinkTransactionCloser();
        sinkTransactionCloser.open(new NoWAL());
    }

    protected abstract void configureTransactionalEnvironment(TransactionEnvironment tEnv);

    @After
    public void tearDown() throws Exception {
        openOperatorTransactionCloser.close();
        stateOperatorTransactionCloser.close();
        sinkTransactionCloser.close();
    }

    private boolean noMoreMessagesInQueue(AbstractListener<?> abstractListener) throws InterruptedException {
        Object receive = abstractListener.receive();
        return receive == null;
    }

    @Test
    public void simpleTest() throws Exception {
        AbstractTwoPCParticipant.SubscriptionMode subscriptionMode = tRuntimeContext.getSubscriptionMode();

        AtOpenListener openListener = new AtOpenListener(openOperatorTransactionCloser, subscriptionMode);
        Address coordinatorAddress = openOperatorTransactionCloser.getServerAddress();
        AtStateListener stateListener = new AtStateListener(
                coordinatorAddress, stateOperatorTransactionCloser, subscriptionMode);

        openListener.subscribeTo(1);
        openListener.subscribeTo(2);
        openListener.subscribeTo(3);
        stateListener.subscribeTo(1);
        stateListener.subscribeTo(2);
        stateListener.subscribeTo(3);

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
        AbstractTwoPCParticipant.SubscriptionMode subscriptionMode = tRuntimeContext.getSubscriptionMode();
        AtOpenListener openListener = new AtOpenListener(openOperatorTransactionCloser, subscriptionMode);
        Address coordinatorAddress = openOperatorTransactionCloser.getServerAddress();

        AtStateListener[] stateListeners = new AtStateListener[10];
        for (int i = 0; i < stateListeners.length; i++) {
            stateListeners[i] = new AtStateListener(
                    coordinatorAddress, stateOperatorTransactionCloser, subscriptionMode);
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
                stateListeners[i].subscribeTo(index + 1);
                // the open is interested in everything
                openListener.subscribeTo(index + 1);
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
        AbstractTwoPCParticipant.SubscriptionMode subscriptionMode = tRuntimeContext.getSubscriptionMode();

        AtOpenListener openListener = new AtOpenListener(openOperatorTransactionCloser, subscriptionMode);
        Address coordinatorAddress = openOperatorTransactionCloser.getServerAddress();

        AtStateListener[] stateListeners = new AtStateListener[10];
        for (int i = 0; i < stateListeners.length; i++) {
            stateListeners[i] = new AtStateListener(
                    coordinatorAddress, stateOperatorTransactionCloser, subscriptionMode);
            stateListeners[i].setVerbose();
        }

        final int numberOfTransactions = 10;
        Metadata[] metas = new Metadata[numberOfTransactions];
        for (int i = 0; i < numberOfTransactions; i++) {
            metas[i] = new Metadata(i + 1);
            metas[i].coordinator = coordinatorAddress;
            metas[i].addCohort(stateOperatorTransactionCloser.getServerAddress());
            // the open is interested in everything
            openListener.subscribeTo(i + 1);
        }

        // every listener is subscribed to every transaction
        for (int i = 0; i < stateListeners.length; i++) {
            for (int j = 0; j < numberOfTransactions; j++) {
                stateListeners[i].subscribeTo(j + 1);
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
                assertEquals((long) j + 1, msg.timestamp);
            }
            assertTrue(noMoreMessagesInQueue(listener));
        }
    }
}
