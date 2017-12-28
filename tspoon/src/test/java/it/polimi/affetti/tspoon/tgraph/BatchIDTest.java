package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by affo on 20/11/17.
 */
public class BatchIDTest {
    public static List<BatchID> generateIdSpace(int steps, int maxChildren, int id) {
        Random rand = new Random(id);
        return generateIdSpace(steps, rand, maxChildren, Collections.singletonList(new BatchID(id)));
    }

    private static List<BatchID> generateIdSpace(int steps, Random rand, int maxChildren, List<BatchID> currentBatchIds) {
        if (steps == 0) {
            return currentBatchIds;
        }

        List<BatchID> space = new LinkedList<>();

        for (BatchID id : currentBatchIds) {
            int size = rand.nextInt(maxChildren - 1) + 1;
            // cloning to make any batchID independent
            List<BatchID> newStep = id.clone().addStep(size);
            newStep.forEach(bid -> space.add(bid.clone()));
        }

        return generateIdSpace(steps - 1, rand, maxChildren, space);
    }

    @Test
    public void newStepTest() {
        BatchID batchID = new BatchID(1);

        for (int i = 1; i <= 10; i++) {
            List<BatchID> newBatch = batchID.addStep(i);
            assertEquals(i, newBatch.size());
            batchID = newBatch.get(0);
        }

        assertEquals(11, batchID.getNumberOfSteps());
    }

    @Test
    public void iterateTest() {
        BatchID batchID = new BatchID(1);
        batchID = batchID.addStep(5).get(0).addStep(3).get(0);

        assertEquals(3, batchID.getNumberOfSteps());

        Iterator<Tuple2<Integer, Integer>> iterator = batchID.iterator();
        assertTrue(iterator.hasNext());
        Tuple2<Integer, Integer> next = iterator.next();
        assertEquals(next, Tuple2.of(1, 1));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertEquals(next, Tuple2.of(1, 5));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertEquals(next, Tuple2.of(1, 3));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testShift() {
        BatchID batchID = new BatchID(1);
        batchID = batchID.addStep(5).get(0).addStep(3).get(0);

        batchID = batchID.getShiftedRepresentation();
        assertEquals(3, batchID.getNumberOfSteps());

        Iterator<Tuple2<Integer, Integer>> iterator = batchID.iterator();
        assertTrue(iterator.hasNext());
        Tuple2<Integer, Integer> next = iterator.next();
        assertEquals(next, Tuple2.of(1, 5));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertEquals(next, Tuple2.of(1, 3));
        assertTrue(iterator.hasNext());
        next = iterator.next();
        assertEquals(next, Tuple2.of(1, 0));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void oneStepCompletenessTest() {
        BatchCompletionChecker completionChecker = new BatchCompletionChecker();

        BatchID first = new BatchID(1);
        List<BatchID> idSpace = new LinkedList<>();
        for (BatchID bid : first.addStep(100)) {
            idSpace.add(bid.clone());
        }

        Collections.shuffle(idSpace);
        BatchID lastOne = idSpace.remove(0);

        for (BatchID batchID : idSpace) {
            assertFalse(completionChecker.checkCompleteness(batchID));
        }

        assertTrue(completionChecker.checkCompleteness(lastOne));
    }

    @Test
    public void completenessTest() {
        BatchCompletionChecker completionChecker = new BatchCompletionChecker();

        int numberOfSteps = 3;
        int maxChildren = 10;
        Random rand = new Random(0);

        List<BatchID> idSpace = generateIdSpace(numberOfSteps, rand, maxChildren,
                Collections.singletonList(new BatchID(1)));

        Collections.shuffle(idSpace);
        BatchID lastOne = idSpace.remove(0);

        for (BatchID batchID : idSpace) {
            assertFalse(completionChecker.checkCompleteness(batchID));
        }

        assertTrue(completionChecker.checkCompleteness(lastOne));
    }
}
