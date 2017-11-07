package it.polimi.affetti.tspoon.tgraph;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 07/11/17.
 */
public class VoteTest {

    private Vote merge(List<Vote> votes) {
        return votes.stream().reduce(Vote::merge).orElseThrow(() -> new RuntimeException("Empty votes"));
    }

    @Test
    public void testAtLeastOneReplay() {
        List<Vote> votes = Arrays.asList(
                Vote.COMMIT,
                Vote.COMMIT,
                Vote.COMMIT,
                Vote.ABORT,
                Vote.REPLAY,
                Vote.COMMIT
        );

        assertEquals(merge(votes), Vote.REPLAY);
    }

    @Test
    public void testAbort() {
        List<Vote> votes = Arrays.asList(
                Vote.COMMIT,
                Vote.COMMIT,
                Vote.COMMIT,
                Vote.ABORT,
                Vote.COMMIT
        );

        assertEquals(merge(votes), Vote.ABORT);
    }

    @Test
    public void testCommits() {
        List<Vote> votes = Arrays.asList(
                Vote.COMMIT,
                Vote.COMMIT,
                Vote.COMMIT,
                Vote.COMMIT
        );

        assertEquals(merge(votes), Vote.COMMIT);
    }
}
