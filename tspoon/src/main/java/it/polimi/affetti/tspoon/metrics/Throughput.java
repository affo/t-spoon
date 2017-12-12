package it.polimi.affetti.tspoon.metrics;

/**
 * Created by affo on 12/12/17.
 */
public class Throughput extends Metric {
    private final int batchSize, every;
    private int count;
    private Long lastTS;

    public Throughput(int batchSize, int every) {
        if (batchSize % every != 0) {
            throw new RuntimeException("Batch size must be a multiple of sub batch size: " +
                    batchSize + " % " + every + " != 0");
        }

        this.batchSize = batchSize;
        this.every = every;
        this.count = 0;
        this.lastTS = null;
    }

    /**
     * @return true if the batch is closed
     */
    public boolean add() {
        if (lastTS == null) {
            // first addition
            lastTS = System.currentTimeMillis();
        }

        count++;

        if (count % every == 0) {
            long newTS = System.currentTimeMillis();
            double elapsedTime = (double) (newTS - lastTS);
            add(every / (elapsedTime / 1000));
            lastTS = newTS;
        }

        return count == batchSize;
    }

    public int getCount() {
        return count;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
