package it.polimi.affetti.tspoon.tgraph.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Created by affo on 16/11/17.
 */
public class KeyLevelExecutorTest {
    private final static long DEADLOCK_TIME = 5L;

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

    private void checkNext(Tuple2<Long, Integer>... results) throws InterruptedException {
        checkNextRec(Stream.of(results).collect(Collectors.toList()));
    }

    private void checkNextRec(List<Tuple2<Long, Integer>> results) throws InterruptedException {
        if (results.isEmpty()) {
            return;
        }

        TaskResult taskResult = observer.get();
        Iterator<Tuple2<Long, Integer>> iterator = results.iterator();
        while (iterator.hasNext()) {
            Tuple2<Long, Integer> next = iterator.next();
            if (next.f0 == taskResult.tid) {
                assertEquals(next.f1, taskResult.result);
                iterator.remove();
                checkNextRec(results);
                return;
            }
        }

        throw new IllegalStateException("Not FIFO");
    }

    private void checkEverythingIsLocked() throws InterruptedException {
        // 4 times because why not
        assertNull(observer.get());
        assertNull(observer.get());
        assertNull(observer.get());
        assertNull(observer.get());
    }

    @Test
    public void testSimpleFIFO() throws InterruptedException {
        executor.startProcessing();

        long foo1id = executor.run(foo, () -> new TaskResult(1));
        long foo2id = executor.run(foo, () -> new TaskResult(3));

        long bar1id = executor.run(bar, () -> new TaskResult(2));
        long bar2id = executor.run(bar, () -> new TaskResult(5));
        long bar3id = executor.run(bar, () -> new TaskResult(6));

        long buz1id = executor.run(buz, () -> new TaskResult(4));

        checkNext(
                Tuple2.of(foo1id, 1),
                Tuple2.of(bar1id, 2),
                Tuple2.of(buz1id, 4));

        // every key is locked now!
        checkEverythingIsLocked();

        // lets free foo
        executor.free(foo);
        assertEquals(3, observer.get().result.intValue());

        checkEverythingIsLocked();

        //let's free the others
        executor.free(buz);
        executor.free(bar);
        assertEquals(5, observer.get().result.intValue());
        executor.free(bar);
        assertEquals(6, observer.get().result.intValue());

        checkEverythingIsLocked();
    }

    @Test
    public void testReadOnly() throws InterruptedException {
        executor.startProcessing();

        TaskResult foo1Tr = new TaskResult(1);
        TaskResult foo2Tr = new TaskResult(2);
        foo2Tr.setReadOnly();

        executor.run(foo, () -> foo1Tr);
        executor.run(foo, () -> foo2Tr);

        assertEquals(1, observer.get().result.intValue());
        checkEverythingIsLocked();

        // 1 is not readOnly, so a synch run should fail!
        assertNull(executor.runSynchronously(foo, () -> 42, 1));
        executor.free(foo);
        // ok, now it should work even if the second task is executing!
        assertEquals(42, executor.runSynchronously(foo, () -> 42, 1).intValue());
        assertEquals(2, observer.get().result.intValue());
        assertEquals(42, executor.runSynchronously(foo, () -> 42, 1).intValue());
        executor.free(foo);

        checkEverythingIsLocked();
    }

    @Test
    public void testDeadlock() throws InterruptedException {
        executor.enableDeadlockDetection(DEADLOCK_TIME);
        executor.startProcessing();

        executor.run(foo, () -> new TaskResult(1));
        // faster then timeout
        assertEquals(1, observer.get().result.intValue());
        // foo is still locked
        executor.run(foo, () -> new TaskResult(1));

        // slower
        Thread.sleep(DEADLOCK_TIME + 10);
        // see #onTaskDeadlock implementation below
        assertEquals(-1, observer.get().result.intValue());

        checkEverythingIsLocked();
        assertEquals(0, executor.getNumberOfEnqueuedTasks());
    }

    @Test
    public void stressTest() throws InterruptedException {
        executor.startProcessing();

        int numberOfTasks = 1000;
        String[] keys = {foo, bar, buz};
        Random rand = new Random(0);
        for (int i = 0; i < numberOfTasks; i++) {
            String key = keys[rand.nextInt(keys.length)];
            int finalI = i;
            executor.run(key, () -> new TaskResult(finalI, (tr) -> {
                try {
                    Thread.sleep(rand.nextInt(9) + 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                executor.free(key);
            }));
        }

        for (int i = 0; i < numberOfTasks; i++) {
            assertNotNull(observer.getUndefinitely());
        }

        checkEverythingIsLocked();
        assertEquals(0, executor.getNumberOfEnqueuedTasks());
    }

    private static class TaskResult extends KeyLevelTaskExecutor.TaskResult {
        public long tid;
        public final Integer result;
        public Consumer<TaskResult> callback;

        public TaskResult(int result) {
            this.result = result;
        }

        public TaskResult(Integer result, Consumer<TaskResult> callback) {
            this.result = result;
            this.callback = callback;
        }
    }

    private static class Observer implements KeyLevelTaskExecutor.TaskCompletionObserver<TaskResult> {
        private BlockingQueue<TaskResult> results = new LinkedBlockingQueue<>();

        public TaskResult get() throws InterruptedException {
            return results.poll(1, TimeUnit.MILLISECONDS);
        }

        public TaskResult getUndefinitely() throws InterruptedException {
            return results.take();
        }

        @Override
        public void onTaskCompletion(long id, TaskResult taskResult) {
            taskResult.tid = id;
            results.add(taskResult);

            if (taskResult.callback != null) {
                taskResult.callback.accept(taskResult);
            }
        }

        @Override
        public void onTaskDeadlock(long id) {
            TaskResult taskResult = new TaskResult(-1);
            taskResult.tid = id;
            results.add(taskResult);
        }
    }
}
