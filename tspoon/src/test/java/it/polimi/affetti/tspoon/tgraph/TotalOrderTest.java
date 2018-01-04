package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by affo on 29/12/17.
 */
public class TotalOrderTest {
    private TotalOrderEnforcer totalOrderEnforcer;

    @Before
    public void setUp() {
        totalOrderEnforcer = new TotalOrderEnforcer();
    }

    @Test
    public void simpleOrderTest() {
        int seed = 1;
        List<BatchID> idSpace = BatchIDTest.generateIdSpace(1, 100, seed);

        Collections.shuffle(idSpace);
        BatchID lastOne = idSpace.remove(0);

        for (BatchID bid : idSpace) {
            totalOrderEnforcer.addElement(seed, bid);
        }

        assertTrue(totalOrderEnforcer.next().isEmpty());

        totalOrderEnforcer.addElement(seed, lastOne);

        int i = 1;
        Iterable<BatchID> batchIDS = totalOrderEnforcer.next().get(seed);
        for (BatchID bid : batchIDS) {
            Iterator<Tuple2<Integer, Integer>> bidIterator = bid.iterator();
            Tuple2<Integer, Integer> next = bidIterator.next();
            assertEquals(1, next.f1.intValue());

            next = bidIterator.next();
            assertEquals(i, next.f0.intValue());
            assertEquals(idSpace.size() + 1, next.f1.intValue());
            i++;
        }
    }

    @Test
    public void testOrder() {
        int steps = 3;
        int maxChildren = 5;
        int noTransactions = 10;

        List<BatchID> idSpace =
                IntStream
                        .range(1, noTransactions + 1)
                        .boxed()
                        .flatMap(i -> BatchIDTest.generateIdSpace(steps, maxChildren, i).stream())
                        .collect(Collectors.toList());

        Collections.shuffle(idSpace);

        // add every element
        for (BatchID bid : idSpace) {
            int timestamp = bid.iterator().next().f0;
            totalOrderEnforcer.addElement(timestamp, bid);
        }

        // ----------------------------- check invariants
        assertEquals(noTransactions, totalOrderEnforcer.getNumberOfIncompleteElements());
        assertEquals(idSpace.size(), totalOrderEnforcer.getTotalNumberOfIncompleteElements());

        int previousTS = 0;
        for (Map.Entry<Integer, List<BatchID>> bids : totalOrderEnforcer.next().entrySet()) {
            int timestamp = bids.getKey();

            assertTrue(timestamp >= previousTS);
            previousTS = timestamp;

            BatchID previousBid = null;
            for (BatchID currentBid : bids.getValue()) {

                if (previousBid != null) {
                    assertTrue(previousBid.compareTo(currentBid) < 0);
                }
                previousBid = currentBid;
            }
        }

        assertEquals(0, totalOrderEnforcer.getNumberOfIncompleteElements());
        assertEquals(0, totalOrderEnforcer.getTotalNumberOfIncompleteElements());
    }
}
