package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.metrics.Throughput;
import it.polimi.affetti.tspoon.metrics.TimeDelta;

import java.util.concurrent.TimeoutException;

/**
 * Created by affo on 07/12/17.
 *
 * Thread-safe
 */
public class MetricCalculator {
    private TimeDelta currentLatency;
    private Throughput currentInputRate, currentThroughput;
    private int numRecords;

    public MetricCalculator() {
        resetMetrics();
    }

    private void resetMetrics() {
        // we calculate the inputRate only on starting records
        currentInputRate = new Throughput("ActualInputRate");
        // we calculate the throughput on every record (start and end) of the batch
        currentThroughput = new Throughput("Throughput");

        if (currentLatency == null) {
            currentLatency = new TimeDelta();
        } else {
            currentLatency.resetMetric();
        }

        numRecords = 0;
    }

    public synchronized void start() {
        resetMetrics();
        currentThroughput.open();
        currentInputRate.open();
    }

    public synchronized Measurement end() {
        currentInputRate.close();
        currentThroughput.close();

        double lastInputRate = currentInputRate.getThroughput();
        double lastThroughput = currentThroughput.getThroughput();
        double lastLatency = currentLatency.getMeanValue();

        return new Measurement(lastInputRate, lastThroughput, lastLatency);
    }

    public synchronized Measurement endAfterCompletion(long timeout) throws InterruptedException, TimeoutException {
        waitForEveryRecordUntilThisPoint(timeout);
        return this.end();
    }

    public synchronized void sent(String id) {
        currentLatency.start(id);
        currentInputRate.processed();
        numRecords++;
    }

    public synchronized void received(String id) {
        boolean startedHere = currentLatency.end(id);
        currentThroughput.processed();

        if (startedHere) {
            numRecords--;
            notifyAll();
        }
    }

    public synchronized void waitForEveryRecordUntilThisPoint(long timeout)
            throws InterruptedException, TimeoutException {
        double start = System.nanoTime();
        long timeLeft = timeout;
        while (numRecords != 0) {
            // sleep only for the time left
            wait(timeLeft);
            // update the time left
            double deltaMs = (System.nanoTime() - start) * Math.pow(10, -6);
            timeLeft = Math.round(timeout - deltaMs);

            if (timeLeft <= 0) {
                throw new TimeoutException();
            }
        }
    }

    public static class Measurement {
        public final double inputRate, throughput, latency;

        private Measurement(double inputRate, double throughput, double latency) {
            this.inputRate = inputRate;
            this.throughput = throughput;
            this.latency = latency;
        }

        public boolean isValid() {
            return !Double.isNaN(latency);
        }

        @Override
        public String toString() {
            return "Measurement{" +
                    "inputRate=" + inputRate +
                    ", throughput=" + throughput +
                    ", latency=" + latency +
                    '}';
        }
    }
}
