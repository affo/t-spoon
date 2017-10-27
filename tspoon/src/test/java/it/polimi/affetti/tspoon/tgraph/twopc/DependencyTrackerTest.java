package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 03/08/17.
 */
public class DependencyTrackerTest {
    private StrictnessEnforcer.DependencyTracker dependencyTracker;

    @Before
    public void setUp() {
        dependencyTracker = new StrictnessEnforcer.DependencyTracker();
    }

    @Test
    public void monotonicSeries() {
        List<Metadata> metadataList = Arrays.asList(
                new Metadata(1),
                new Metadata(2),
                new Metadata(3),
                new Metadata(4),
                new Metadata(5)
        );

        for (Metadata metadata : metadataList) {
            dependencyTracker.addMetadata(metadata);
        }

        assertEquals(metadataList, dependencyTracker.next());
        assertEquals(Collections.emptyList(), dependencyTracker.next());
    }

    @Test
    public void waitingForSomething() {
        Metadata m1 = new Metadata(1);
        Metadata m2 = new Metadata(2);
        m2.dependencyTracking.add(4);

        dependencyTracker.addMetadata(m1);
        dependencyTracker.addMetadata(m2);
        assertEquals(Arrays.asList(m1, m2), dependencyTracker.next());
        assertEquals(Collections.emptyList(), dependencyTracker.next()); // empty...

        Metadata m5 = new Metadata(5);
        dependencyTracker.addMetadata(m5);
        assertEquals(Collections.emptyList(), dependencyTracker.next()); // 5 is not contiguous! Still waiting for 3

        Metadata m4 = new Metadata(4);
        dependencyTracker.addMetadata(m4);
        List<Metadata> actual = dependencyTracker.next();
        assertEquals(Collections.singletonList(m4), actual);
        assertEquals(Vote.REPLAY, actual.get(0).vote); // 2 saw it, now it needs replay!

        Metadata m3 = new Metadata(3);
        dependencyTracker.addMetadata(m3);
        assertEquals(Collections.singletonList(m3), dependencyTracker.next()); // contiguous with 2, but still waiting for 4

        // m4 is a commit
        m4.vote = Vote.COMMIT;
        dependencyTracker.addMetadata(m4); // 4 has been replayed
        assertEquals(Arrays.asList(m4, m5), dependencyTracker.next()); // OK

        assertEquals(Collections.emptyList(), dependencyTracker.next()); // empty...
    }
}
