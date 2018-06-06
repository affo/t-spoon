package it.polimi.affetti.tspoon.metrics;

import java.io.Serializable;

/**
 * Created by affo on 12/12/17.
 */
public class Throughput implements Serializable {
    private final String label;
    private Long lastTS;
    private double throughput;
    private long count;

    public Throughput() {
        this("");
    }

    public Throughput(String label) {
        this.label = label;
        this.lastTS = null;
        this.count = 0;
    }

    public void open() {
        lastTS = System.nanoTime();
    }

    public void processed() {
        count++;
    }

    public void close() {
        long elapsedTime = System.nanoTime() - lastTS;
        this.throughput = count / (elapsedTime * Math.pow(10, -9));
    }

    public double getThroughput() {
        return throughput;
    }
}
