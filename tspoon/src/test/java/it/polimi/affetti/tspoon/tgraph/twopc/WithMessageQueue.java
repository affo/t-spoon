package it.polimi.affetti.tspoon.tgraph.twopc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 02/12/17.
 */
public abstract class WithMessageQueue<M> {
    public final static long DEFAULT_TIMEOUT = 10L;
    private final BlockingQueue<M> received = new LinkedBlockingQueue<>();
    private boolean verbose = false;
    private String verbosePrefix = "";

    public void setVerbose(String prefix) {
        this.verbosePrefix = prefix;
        this.verbose = true;
    }

    public void addMessage(M message) {
        if (verbose) {
            System.out.println(
                    String.format("> %s -- %s\t%s",
                            Thread.currentThread().getName(),
                            verbosePrefix,
                            message.toString()));
        }
        received.add(message);
    }

    public M receive() throws InterruptedException {
        return this.receive(DEFAULT_TIMEOUT);
    }

    public M receive(long timeout) throws InterruptedException {
        return received.poll(timeout, TimeUnit.MILLISECONDS);
    }
}
