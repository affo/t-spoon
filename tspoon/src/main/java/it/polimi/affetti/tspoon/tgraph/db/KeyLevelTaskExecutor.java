package it.polimi.affetti.tspoon.tgraph.db;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.function.Supplier;

/**
 * Executes the provided tasks at #run with a key-level locking.
 * I.e., tasks are executed in FIFO order wrt the keys provided on #run.
 * Keys needs to be explicitly unlocked by invoking #complete.
 *
 * A key is supposed to be exclusively locked until a task with that key is running.
 * The KeyLevelTaskExecutor uses the taskIDs provided at #run in order to reorder task execution.
 * The KeyLevelTaskExecutor raises an OutOfOrderTaskException if it is provided with a task with a smaller ID
 * than the ID of the last executed task for the same key. This means, indeed, that the task should be retried
 * with a bigger taskID.
 * Otherwise, the KeyLevelTaskExecutor keeps queues for every key provided and it enqueues the tasks in-order
 * wrt their queue.
 *
 * @param <R> the type of the task result
 */
public class KeyLevelTaskExecutor<R> {
    private final Logger LOG = Logger.getLogger(KeyLevelTaskExecutor.class);

    private final int numberOfExecutors;
    private transient List<Tuple2<SerialExecutor, Thread>> pool;

    // These queues contains tasks from `run` to `complete`
    private final Map<String, List<Task>> queues = new HashMap<>();
    private final Map<String, Long> lastExecutedTaskID = new HashMap<>();
    private final Set<String> freeKeys = new HashSet<>();
    private final TaskCompletionObserver<R> observer;

    private boolean stop = false;

    public KeyLevelTaskExecutor(int numberOfExecutors, TaskCompletionObserver<R> observer) {
        this.numberOfExecutors = numberOfExecutors;
        this.observer = observer;
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
    }

    private synchronized void stop() {
        this.stop = true;
        notifyAll();
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
    }

    private class Task {
        public final long creationTime;
        public final long id;
        public final String key;
        public final Supplier<R> actualTask; // returns the id of the task
        public boolean executed = false;

        public Task(long taskID, String key, Supplier<R> actualTask) {
            this.creationTime = System.currentTimeMillis();
            this.id = taskID;
            this.key = key;
            this.actualTask = actualTask;
        }

        public synchronized R execute() {
            return actualTask.get();
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
            Iterator<String> keysIterator = freeKeys.iterator();
            while (keysIterator.hasNext()) {
                String freeKey = keysIterator.next();
                List<Task> tasks = queues.get(freeKey);
                if (!tasks.isEmpty()) {
                    keysIterator.remove();
                    Task task = tasks.get(0); // the oldest for this key
                    // once a task is taken from the queue it is considered as executed
                    task.executed = true;

                    if (task.id > lastExecutedTaskID.getOrDefault(task.key, Long.MIN_VALUE)) {
                        lastExecutedTaskID.put(task.key, task.id);
                    }

                    return task;
                }
            }

            // If no key or no task is available, just wait
            wait();
        }

        return null;
    }

    /**
     * Executes the task with the provided taskID (once possible, i.e. when its key is free).
     *
     * Raises an OutOfOrderTaskException if a task with a smaller ID than the ID of the last executed task is provided.
     * Otherwise it enqueues the task in order wrt the IDs for the specified key.
     *
     * @param key
     * @param task
     * @return
     */
    public synchronized void run(long taskID, String key, Supplier<R> task)
            throws OutOfOrderTaskException {
        long lastExecutedID = lastExecutedTaskID.getOrDefault(key, Long.MIN_VALUE);
        if (taskID < lastExecutedID) {
            throw new OutOfOrderTaskException(lastExecutedID,
                    "TaskID provided is " + taskID + ", while last executed one's is " + lastExecutedID);
        }

        // add in execution order
        ListIterator<Task> iterator = queues.computeIfAbsent(key, k -> {
            freeKeys.add(k);
            return new LinkedList<>();
        }).listIterator();

        while (iterator.hasNext()) {
            Task inQueue = iterator.next();
            if (inQueue.id > taskID) {
                iterator.previous();
                break;
            }
        }

        Task actualTask = new Task(taskID, key, task);
        iterator.add(actualTask);

        notifyAll();
    }

    /**
     * Completes the task and frees the corresponding key.
     * @param taskID
     */
    public synchronized void complete(String key, long taskID) {
        if (freeKeys.contains(key)) {
            throw new IllegalStateException("Cannot unlock a free key: " + key + "-" + taskID);
        }

        List<Task> tasks = queues.get(key);
        Task task = tasks.get(0);
        if (task.id != taskID) {
            throw new IllegalStateException("Task not found: " + key + "-" + taskID);
        }

        if (!task.executed) {
            throw new IllegalStateException("The completed task has not yet been executed: " + key + "-" + taskID);
        }

        tasks.remove(0);
        freeKeys.add(key); // next task in queue can proceed
        notifyAll();
    }

    public synchronized int getNumberOfEnqueuedTasks() {
        return queues.values().stream().mapToInt(List::size).sum();
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

                    R taskResult = task.execute();
                    observer.onTaskCompletion(task.key, task.id, taskResult);
                }
            } catch (InterruptedException e) {
                LOG.error("Serial executor interrupted while running: " + e.getMessage());
            }
        }
    }

    public interface TaskCompletionObserver<R> {
        void onTaskCompletion(String key, long id, R taskResult);
    }

    public static class OutOfOrderTaskException extends Exception {
        private final long greaterID;

        public OutOfOrderTaskException(long greaterID, String message) {
            super(message);
            this.greaterID = greaterID;
        }

        public long getGreaterID() {
            return greaterID;
        }
    }
}
