package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.KeyLevelTaskExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * Created by affo on 16/11/17.
 */
public class KeyLevelExecutorTest {
    private KeyLevelTaskExecutor<TaskResult> executor;
    private Observer observer;
    // some keys;
    private final String foo = "foo", bar = "bar", buz = "buz";

    @Before
    public void setUp() {
        observer = new Observer();
        executor = new KeyLevelTaskExecutor<>(4, observer);
    }

    @After
    public void tearDown() {
        executor.stopProcessing();
    }

    private void checkEverythingIsLocked() throws InterruptedException {
        for (String key : Arrays.asList(foo, bar, buz)) {
            // 4 times because why not
            assertNull(observer.get(key));
            assertNull(observer.get(key));
            assertNull(observer.get(key));
            assertNull(observer.get(key));
        }
    }

    @Test
    public void testSimpleFIFO() throws InterruptedException, KeyLevelTaskExecutor.OutOfOrderTaskException {
        executor.startProcessing();

        executor.run(1, foo, () -> new TaskResult(1));
        executor.run(2, foo, () -> new TaskResult(3));

        executor.run(1, bar, () -> new TaskResult(2));
        executor.run(2, bar, () -> new TaskResult(5));
        executor.run(3, bar, () -> new TaskResult(6));

        executor.run(1, buz, () -> new TaskResult(4));

        assertEquals(1, observer.getUndefinitely(foo).result);
        assertEquals(2, observer.getUndefinitely(bar).result);
        assertEquals(4, observer.getUndefinitely(buz).result);

        // every key is locked now!
        checkEverythingIsLocked();

        // lets free foo
        executor.complete(foo, 1);
        assertEquals(3, observer.getUndefinitely(foo).result);

        checkEverythingIsLocked();

        //let's free the others
        executor.complete(buz, 1);
        executor.complete(bar, 1);
        assertEquals(5, observer.getUndefinitely(bar).result);
        executor.complete(bar, 2);
        assertEquals(6, observer.getUndefinitely(bar).result);

        executor.complete(bar, 3);

        checkEverythingIsLocked();
    }

    @Test
    public void stressTest() throws InterruptedException, KeyLevelTaskExecutor.OutOfOrderTaskException {
        executor.startProcessing();

        int numberOfTasks = 1000;
        String[] keys = {foo, bar, buz};
        Map<String, Integer> numberOfTasksPerKey = new HashMap<>();
        numberOfTasksPerKey.put(foo, 0);
        numberOfTasksPerKey.put(bar, 0);
        numberOfTasksPerKey.put(buz, 0);

        Random rand = new Random(0);
        for (int i = 0; i < numberOfTasks; i++) {
            String key = keys[rand.nextInt(keys.length)];
            numberOfTasksPerKey.compute(key, (k, v) -> v + 1);

            executor.run(i, key, () -> new TaskResult(42,
                    tr -> {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextInt(9) + 1);
                            executor.complete(key, tr.tid);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }));
        }

        for (String key : numberOfTasksPerKey.keySet()) {
            for (int i = 0; i < numberOfTasksPerKey.get(key); i++) {
                assertNotNull(observer.getUndefinitely(key));
            }
        }

        checkEverythingIsLocked();
        assertEquals(0, executor.getNumberOfEnqueuedTasks());
    }

    @Test
    public void outOfOrderTest() throws KeyLevelTaskExecutor.OutOfOrderTaskException, InterruptedException {
        executor.startProcessing();

        // just to lock foo
        executor.run(1, foo, () -> new TaskResult(1));

        // this ones will be enqueued without any problem
        executor.run(5, foo, () -> new TaskResult(5));
        executor.run(3, foo, () -> new TaskResult(3));
        executor.run(2, foo, () -> new TaskResult(2));
        executor.run(7, foo, () -> new TaskResult(7));

        // and executed in order
        assertEquals(1, observer.getUndefinitely(foo).result);
        executor.complete(foo, 1);
        assertEquals(2, observer.getUndefinitely(foo).result);
        executor.complete(foo, 2);
        assertEquals(3, observer.getUndefinitely(foo).result);
        executor.complete(foo, 3);
        assertEquals(5, observer.getUndefinitely(foo).result);
        executor.complete(foo, 5);
        assertEquals(7, observer.getUndefinitely(foo).result);
        executor.complete(foo, 7);

        checkEverythingIsLocked();
        assertEquals(0, executor.getNumberOfEnqueuedTasks());
    }

    @Test(expected = KeyLevelTaskExecutor.OutOfOrderTaskException.class)
    public void outOfOrderWithViolationTest() throws KeyLevelTaskExecutor.OutOfOrderTaskException, InterruptedException {
        executor.startProcessing();

        executor.run(5, foo, () -> new TaskResult(42));
        Thread.sleep(100); // task 5 will be executed
        executor.run(1, foo, () -> new TaskResult(42)); // raises the exception
    }

    private static class TaskResult {
        public long tid;
        public final int result;
        public Consumer<TaskResult> callback;

        public TaskResult(int result) {
            this.result = result;
        }

        public TaskResult(int result, Consumer<TaskResult> callback) {
            this.result = result;
            this.callback = callback;
        }
    }

    private static class Observer implements KeyLevelTaskExecutor.TaskCompletionObserver<TaskResult> {
        private Map<String, BlockingQueue<TaskResult>> results = new HashMap<>();

        private synchronized BlockingQueue<TaskResult> getQueue(String key) {
            return results.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
        }

        public TaskResult get(String key) throws InterruptedException {
            return getQueue(key).poll(1, TimeUnit.MILLISECONDS);
        }

        public TaskResult getUndefinitely(String key) throws InterruptedException {
            return getQueue(key).take();
        }

        @Override
        public void onTaskCompletion(String key, long id, TaskResult taskResult) {
            taskResult.tid = id;
            getQueue(key).add(taskResult);

            if (taskResult.callback != null) {
                taskResult.callback.accept(taskResult);
            }
        }
    }
}
