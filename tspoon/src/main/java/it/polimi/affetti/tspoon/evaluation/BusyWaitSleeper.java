package it.polimi.affetti.tspoon.evaluation;

import java.io.Serializable;

public class BusyWaitSleeper implements Serializable {
    private final long maxSleep;
    private final long minSleep;

    public BusyWaitSleeper(long maxSleep, long minSleep) {
        this.maxSleep = maxSleep;
        this.minSleep = minSleep;
    }

    public long sleep() {
        if (maxSleep <= 0 || minSleep < 0) {
            return 0L;
        }
        long sleepTime = (long) (Math.random() * maxSleep) + minSleep;
        long currentTime = System.nanoTime();
        // busy wait
        while (System.nanoTime() - currentTime < sleepTime * 1000) ;
        return sleepTime;
    }
}
