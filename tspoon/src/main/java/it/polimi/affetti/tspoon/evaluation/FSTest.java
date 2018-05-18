package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.Metric;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.durability.FileWAL;
import it.polimi.affetti.tspoon.tgraph.durability.WALEntry;

import java.io.IOException;

/**
 * Created by affo on 18/05/18.
 */
public class FSTest {
    private static Metric metric = new Metric();
    private static WALEntry entry;

    static {
        Updates updates = new Updates();
        updates.addUpdate("ns", "k1", 42);
        updates.addUpdate("ns", "k2", 43);
        entry = new WALEntry(Vote.COMMIT, -1, -1, updates);
    }

    private static void addEntry(FileWAL wal) {
        long start = System.nanoTime();
        wal.addEntry(entry);
        double delta = (System.nanoTime() - start) * Math.pow(10, -6); // ms
        metric.add(delta);
    }

    public static void main(String[] args) throws IOException {
        FileWAL wal = new FileWAL("fstest.log", true);
        wal.open();

        for (int i = 0; i < 100000; i++) {
            addEntry(wal);
        }

        System.out.println(metric);
        wal.close();
    }
}
