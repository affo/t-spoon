package it.polimi.affetti.tspoon.metrics;

import java.io.Serializable;

/**
 * Created by affo on 12/12/17.
 */
public class Throughput implements Serializable {
    private final String label;
    private Long lastTS;
    private double throughput;

    public Throughput() {
        this("");
    }

    public Throughput(String label) {
        this.label = label;
        this.lastTS = null;
    }

    public void open() {
        lastTS = System.currentTimeMillis();
    }

    public void close(int batchSize) {
        long newTS = System.currentTimeMillis();
        long elapsedTime = newTS - lastTS;
        this.throughput = batchSize / (elapsedTime / 1000.0);
    }

    public double getThroughput() {
        return throughput;
    }
}
