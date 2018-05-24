package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by affo on 24/05/18.
 */
public class BatchingStringClient extends StringClient {
    public static final long BATCH_TIMEOUT_MILLISECONDS = 1;
    private final int batchSize;
    private long batchNumber = 0;
    private long currentBatchSize;
    private transient Timer flusher;

    public BatchingStringClient(String addr, int port, int batchSize) {
        super(addr, port);
        this.batchSize = batchSize;
        this.currentBatchSize = 0;
        this.batchNumber = 0;
    }

    @Override
    public void init() throws IOException {
        super.init();
        flusher = new Timer();
        flusher.schedule(new FlushTask(batchNumber), BATCH_TIMEOUT_MILLISECONDS, BATCH_TIMEOUT_MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        super.close();
        flusher.cancel();
        flusher.purge();
    }

    @Override
    public synchronized void send(String request) {
        out.println(request);
        currentBatchSize++;
        if (currentBatchSize == batchSize) {
            out.flush();
            newBatch();
        }
    }

    private void newBatch() {
        currentBatchSize = 0;
        batchNumber++;
    }

    private synchronized long flushIf(long lastCheckedBatchNumber) {
        // if 2 checks happened in the same batch, then it needs to be flushed
        if (batchNumber == lastCheckedBatchNumber) {
            out.flush();
            newBatch();
        }
        return batchNumber;
    }

    private class FlushTask extends TimerTask {
        private long lastBatchNumber;

        public FlushTask(long lastBatchNumber) {
            this.lastBatchNumber = lastBatchNumber;
        }

        @Override
        public void run() {
            lastBatchNumber = flushIf(lastBatchNumber);
        }
    }
}
