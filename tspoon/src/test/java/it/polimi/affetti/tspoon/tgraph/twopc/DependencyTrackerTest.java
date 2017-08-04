package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
        assertEquals(Collections.emptyList(), dependencyTracker.next()); // no idea what 4 is

        Metadata m4 = new Metadata(4);
        dependencyTracker.addMetadata(m4);
        assertEquals(Arrays.asList(m1, m2), dependencyTracker.next());

        Metadata m3 = new Metadata(3);
        dependencyTracker.addMetadata(m3);
        List<Metadata> next = dependencyTracker.next();
        List<Integer> expected = Arrays.asList(3, 4);
        List<Integer> actual = next.stream().map(m -> m.tid).collect(Collectors.toList());
        assertEquals(expected, actual); // ok, now 3 is there!

        assertEquals(Vote.COMMIT, next.get(0).vote);
        assertEquals(Vote.REPLAY, next.get(1).vote); // 2 saw it, now it needs replay!

        assertEquals(Collections.emptyList(), dependencyTracker.next());

        Metadata m5 = new Metadata(5);
        dependencyTracker.addMetadata(m5);
        assertEquals(Collections.emptyList(), dependencyTracker.next()); // still have a hole for 4

        // ok, now we have a replay:
        Metadata mReplayed = new Metadata(4);
        dependencyTracker.addMetadata(mReplayed);
        assertEquals(Arrays.asList(mReplayed, m5), dependencyTracker.next());
    }
}
