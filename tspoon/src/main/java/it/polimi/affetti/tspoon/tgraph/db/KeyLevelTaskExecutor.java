package it.polimi.affetti.tspoon.tgraph.db;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.function.Supplier;

/**
 * Executes callables provided with method doRun with a key-level locking.
 * I.e., tasks are executed in FIFO order wrt the keys provided on doRun.
 * Keys needs to be explicitely unlocked by invoking unlock.
 * <p>
 * NOTE: Tasks are "running" from #run(k, ...) to #free(k).
 * The exception is the case for #runSynchronously(k, ...) in which the Task runs for the duration of
 * the method invocation.
 * <p>
 * Once a task is enqueued, its key is supposed to be locked exclusively.
 * After execution, the key's read-only value is updated accordingly to the OperationExecutionResult.
 * <p>
 * Every time an asynchronous #run(k, ...) is issued, the Executor doesn't bother if k is read-only or not.
 * A key, indeed, is supposed to be exclusively locked if a task with that key is running.
 * <p>
 * On synchronous runs (#runSynchronously), keys are checked for being read-only.
 *
 * @param <T> the type of the task result
 */
public class KeyLevelTaskExecutor<T extends KeyLevelTaskExecutor.TaskResult> {
    public final long DEFAULT_MAX_MILLISECONDS_IN_QUEUE = 100L;
    private final Logger LOG = Logger.getLogger(KeyLevelTaskExecutor.class);

    private long taskProgressiveID = 0L;

    private transient List<Tuple2<SerialExecutor, Thread>> pool;
    private final int numberOfExecutors;
    private boolean deadlockDetectionEnabled;
    private long maxTimeInQueue = DEFAULT_MAX_MILLISECONDS_IN_QUEUE;
    private Timer deadlockTimer;
    private boolean notified;

    private final Map<Long, Task> registeredTasks = new HashMap<>();
    private final List<Task> queue = new LinkedList<>();
    private final Map<String, Boolean> lockedKeys = new HashMap<>(); // key -> readOnly
    private final TaskCompletionObserver<T> observer;

    private boolean stop = false;

    public KeyLevelTaskExecutor(int numberOfExecutors, TaskCompletionObserver<T> observer) {
        this.numberOfExecutors = numberOfExecutors;
        this.observer = observer;
    }

    public void enableDeadlockDetection() {
        this.enableDeadlockDetection(DEFAULT_MAX_MILLISECONDS_IN_QUEUE);
    }

    public void enableDeadlockDetection(long maxTimeInQueue) {
        if (deadlockDetectionEnabled) {
            // cannot enable more than once
            return;
        }

        if (maxTimeInQueue <= 0) {
            throw new IllegalArgumentException("Deadlock timeout must be > 0: " + maxTimeInQueue);
        }

        deadlockDetectionEnabled = true;
        this.maxTimeInQueue = maxTimeInQueue;
        this.deadlockTimer = new Timer("Deadlock Timer for " + Thread.currentThread().getName());
    }

    public void startProcessing() {
        pool = new ArrayList<>(numberOfExecutors);

        for (int i = 0; i < numberOfExecutors; i++) {
            SerialExecutor executor = new SerialExecutor();
            Thread thread = new Thread(executor);
            pool.add(Tuple2.of(executor, thread));
            thread.setName("KeyLevelExecutor " + i + " for " + Thread.currentThread().getName());
            thread.start();
        }

        if (deadlockDetectionEnabled) {
            deadlockTimer.schedule(new DeadLockDetector(), maxTimeInQueue, maxTimeInQueue);
        }
    }

    private synchronized void stop() {
        this.stop = true;
        myNotify();
    }

    public void stopProcessing() {
        for (int i = 0; i < numberOfExecutors; i++) {
            stop();
        }

        try {
            for (Tuple2<SerialExecutor, Thread> couple : pool) {
                couple.f1.join();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for KeyLevel executor to finish: " + e.getMessage());
        }

        if (deadlockDetectionEnabled) {
            deadlockTimer.cancel();
            deadlockTimer.purge();
        }
    }

    private class Task {
        public final long creationTime;
        public final long id;
        public final String key;
        public final Supplier<T> actualTask; // returns the id of the task

        public Task(String key, Supplier<T> actualTask) {
            this.creationTime = System.currentTimeMillis();
            this.id = taskProgressiveID;
            this.key = key;
            this.actualTask = actualTask;
            taskProgressiveID++;
        }

        public T execute() {
            return actualTask.get();
        }
    }

    // invoked by the SerialExecutor
    private synchronized Task getAvailableTask() throws InterruptedException {
        while (!stop) {
            // Iterate until a non-blocked task is available
            final Iterator<Task> it = queue.iterator();
            while (it.hasNext()) {
                Task task = it.next();
                if (!lockedKeys.containsKey(task.key)) {
                    lockedKeys.put(task.key, false); // put as not read-only
                    it.remove();
                    return task;
                }
            }

            // If no key is available, just wait
            wait();
            notified = false;
        }

        return null;
    }

    private synchronized Set<Task> removeDeadlockedTasks(long now) {
        Set<Task> removed = new HashSet<>();

        Iterator<Task> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Task next = iterator.next();
            long timePassed = now - next.creationTime;
            if (timePassed > maxTimeInQueue) {
                iterator.remove();
                lockedKeys.remove(next.key);
                removed.add(next);
            }
        }

        if (!removed.isEmpty()) {
            myNotify();
        }

        return removed;
    }

    private synchronized void myNotify() {
        notifyAll();
        notified = true;
    }

    private synchronized void setReadOnly(String key) {
        lockedKeys.put(key, true);
        myNotify();
    }

    /**
     * Sets a key as available
     *
     * @param key
     */
    public synchronized void free(String key) {
        lockedKeys.remove(key);
        myNotify();
    }

    /**
     * adds the task and returns its id.
     *
     * @param key
     * @param task
     * @return
     */
    public synchronized long add(String key, Supplier<T> task) {
        Task actualTask = new Task(key, task);
        registeredTasks.put(actualTask.id, actualTask);
        return actualTask.id;
    }

    /**
     * Executes the task when its key it's free.
     *
     * @param taskId the id of the task
     * @return
     */
    public synchronized void run(long taskId) {
        queue.add(registeredTasks.remove(taskId));
        myNotify();
    }

    /**
     * Used by external, read-only queries
     */
    public synchronized <R> R runSynchronously(String key, Supplier<R> task) {
        return this.runSynchronously(key, task, maxTimeInQueue);
    }

    public synchronized int getNumberOfEnqueuedTasks() {
        return queue.size();
    }

    public synchronized <R> R runSynchronously(String key, Supplier<R> task, long timeout) {
        try {
            while (lockedKeys.containsKey(key) && !lockedKeys.get(key)) {
                wait(timeout);
                if (!notified) {
                    return null;
                }
                notified = false;
            }

            return task.get();

        } catch (InterruptedException e) {
            LOG.error("Serial executor interrupted while synchronously running: " + e.getMessage());
        }

        return null;
    }

    public interface TaskCompletionObserver<R extends TaskResult> {
        void onTaskCompletion(long id, R taskResult);

        default void onTaskDeadlock(long id) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Extend this class for results
     */
    public static class TaskResult {
        public boolean isReadOnly;

        public void setReadOnly() {
            this.isReadOnly = true;
        }
    }

    private class SerialExecutor implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    Task task = getAvailableTask();

                    if (task == null) {
                        break;
                    }

                    T taskResult = task.execute();

                    if (taskResult.isReadOnly) {
                        setReadOnly(task.key);
                    }
                    observer.onTaskCompletion(task.id, taskResult);
                }
            } catch (InterruptedException e) {
                LOG.error("Serial executor interrupted while running: " + e.getMessage());
            }
        }
    }

    private class DeadLockDetector extends TimerTask {
        @Override
        public void run() {
            Set<Task> deadlockedTasks = removeDeadlockedTasks(System.currentTimeMillis());

            for (Task task : deadlockedTasks) {
                observer.onTaskDeadlock(task.id);
            }
        }
    }
}
