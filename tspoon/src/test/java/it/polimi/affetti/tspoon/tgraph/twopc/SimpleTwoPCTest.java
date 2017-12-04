package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by affo on 02/12/17.
 */
public abstract class SimpleTwoPCTest {
    private AbstractOpenOperatorTransactionCloser coordinator;
    private AbstractStateOperationTransactionCloser stateOp;
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
        Address coordinatorAddress = coordinator.getServerAddress();
        AtStateListener stateListener = new AtStateListener(coordinatorAddress);
        coordinator.subscribeTo(1, openListener);
        coordinator.subscribeTo(2, openListener);
        coordinator.subscribeTo(3, openListener);
        stateOp.subscribeTo(1, stateListener);
        stateOp.subscribeTo(2, stateListener);
        stateOp.subscribeTo(3, stateListener);

        openListener.setVerbose();
        stateListener.setVerbose();

        Metadata[] metas = {
                new Metadata(1),
                new Metadata(2),
                new Metadata(3)
        };

        for (int i = 0; i < metas.length; i++) {
            metas[i].coordinator = coordinatorAddress;
            metas[i].addCohort(stateOp.getServerAddress());
        }

        for (int i = 0; i < metas.length; i++) {
            sink.onMetadata(metas[i]);
        }

        Set<Integer> timestamps = IntStream.range(0, metas.length).mapToObj(i -> {
            try {
                return openListener.receive();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;
        }).map(notification -> notification.timestamp).collect(Collectors.toSet());

        assertEquals(Sets.newHashSet(1, 2, 3), timestamps);

        timestamps = IntStream.range(0, metas.length).mapToObj(i -> {
            try {
                return stateListener.receive();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;
        }).map(notification -> notification.timestamp).collect(Collectors.toSet());

        assertEquals(Sets.newHashSet(1, 2, 3), timestamps);

        assertTrue(noMoreMessagesInQueue(openListener));
        assertTrue(noMoreMessagesInQueue(stateListener));
    }
}
