package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;

/**
 * Created by affo on 26/07/17.
 */
public interface JobControlListener {
    default void onJobFinish() {
        // every operator must close the observer
        try {
            JobControlObserver.close();
        } catch (IOException e) {
            throw new RuntimeException("Exception while closing JobControlObserver: " + e.getMessage());
        }
    }

    default void onJobFinishExceptionally(String exceptionMessage) {
        // ignore the message by default
        onJobFinish();
    }

    void onBatchEnd();
}
