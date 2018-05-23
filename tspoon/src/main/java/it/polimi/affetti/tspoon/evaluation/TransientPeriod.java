package it.polimi.affetti.tspoon.evaluation;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by affo on 23/05/18.
 */
public class TransientPeriod implements Serializable {
    private boolean end = false;
    private final int spanInSeconds;

    public TransientPeriod(int spanInSeconds) {
        this.spanInSeconds = spanInSeconds;
    }

    public void start() {
        this.start(() -> {
        });
    }

    public void start(Runnable hook) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                end = true;
                hook.run();
            }
        }, spanInSeconds * 1000);
    }

    public boolean hasFinished() {
        return end;
    }
}
