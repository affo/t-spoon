package it.polimi.affetti.tspoon.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by affo on 26/01/18.
 *
 * Reinventing the wheel, but it does not block the Exceptions.
 */
public class TaskExecutor extends Thread {
    private Logger LOG = Logger.getLogger(TaskExecutor.class.getSimpleName());
    private final BlockingQueue<Tuple2<Runnable, TaskErrorListener>> tasks;

    public TaskExecutor() {
        tasks = new LinkedBlockingQueue<>();
    }

    public void addTask(Runnable task) {
        this.addTask(task, null);
    }

    public void addTask(Runnable task, TaskErrorListener listener) {
        tasks.add(Tuple2.of(task, listener));
    }

    @Override
    public void run() {
        try {
            while (true) {
                TaskErrorListener listener = null;
                try {
                    Tuple2<Runnable, TaskErrorListener> taken = tasks.take();
                    listener = taken.f1;
                    taken.f0.run();
                } catch (InterruptedException e) {
                    throw e;
                } catch (Throwable t) {
                    if (listener != null) {
                        listener.onTaskError(t);
                    } else {
                        throw t;
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while running tasks");
        }
    }

    public interface TaskErrorListener {
        void onTaskError(Throwable t);
    }
}
