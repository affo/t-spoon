package it.polimi.affetti.tspoon.tgraph.db;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
 * NOTE: Either a task deadlocks or executes.
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
    // These queues contains tasks from `run` to `free`
    private final LinkedHashMap<Long, Task> queue = new LinkedHashMap<>();
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
        public long executionTime;
        public final long id;
        public final String key;
        public final Supplier<T> actualTask; // returns the id of the task
        public boolean executed = false;

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

        public void setExecutionTime() {
            this.executionTime = System.currentTimeMillis();
        }

        @Override
        public boolean equals(java.lang.Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Task task = (Task) o;

            return id == task.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    // invoked by the SerialExecutor
    private synchronized Task getAvailableTask() throws InterruptedException {
        while (!stop) {
            // Iterate until a non-blocked task is available
            for (Task task : queue.values()) {
                if (!lockedKeys.containsKey(task.key)) {
                    lockedKeys.put(task.key, false); // put as not read-only
                    task.executed = true;
                    return task;
                }
            }

            // If no key is available, just wait
            wait();
            notified = false;
        }

        return null;
    }

    /**
     * Returns the deadlocked task in task id order (from older to newer)
     * @param now
     * @return
     */
    private synchronized List<Task> removeDeadlockedTasks(long now) {
        List<Task> removed = new LinkedList<>();

        Iterator<Task> iterator = queue.values().iterator();
        while (iterator.hasNext()) {
            Task next = iterator.next();
            long timePassed = now - next.executionTime;
            if (timePassed > maxTimeInQueue) {
                iterator.remove();
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
        removeOldestTask(key);
        lockedKeys.remove(key); // next task in queue can proceed
        myNotify();
    }

    private synchronized Task removeOldestTask(String key) {
        Task oldest = null;
        Iterator<Task> iterator = queue.values().iterator();
        while (iterator.hasNext()) {
            Task next = iterator.next();
            if (next.key.equals(key)) {
                iterator.remove();
                oldest = next;
                break;
            }
        }

        return oldest;
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
        Task task = registeredTasks.remove(taskId);
        queue.put(task.id, task); // put in execution order
        task.setExecutionTime();
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

        /**
         *
         * @param deadlockedWithDependencies a map of the deadlocked tasks with their dependencies
         *                                   (in task id order, i.e., from older to newer).
         *                                   Dependencies consist of every other deadlocked task operating
         *                                   on the same key.
         */
        default void onDeadlock(LinkedHashMap<Long, List<Long>> deadlockedWithDependencies) {
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
            List<Task> deadlockedTasks = removeDeadlockedTasks(System.currentTimeMillis());
            Map<String, List<Long>> byKey = deadlockedTasks.stream()
                    .collect(
                            Collectors.groupingBy(
                                    task -> task.key,
                                    Collectors.mapping(task -> task.id, Collectors.toList())));

            LinkedHashMap<Long, List<Long>> dependencies = new LinkedHashMap<>();
            for (Task deadlocked : deadlockedTasks) {
                // only not executed tasks can deadlock
                if (!deadlocked.executed) {
                    List<Long> dependency = byKey.get(deadlocked.key).stream()
                            .filter(id -> id != deadlocked.id) // the others
                            .collect(Collectors.toList());
                    dependencies.put(deadlocked.id, dependency);
                }
            }

            observer.onDeadlock(dependencies);
        }
    }
}
