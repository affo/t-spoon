package it.polimi.affetti.tspoon.tgraph.functions;

import junit.framework.AssertionFailedError;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Created by affo on 27/07/17.
 */
public class PartitioningTest {

    @Test
    public void testKeyByConnected() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> bcastedElements = Arrays.asList(1, 2, 3, 4, 5);
        String[] strings = {"foo", "bar", "buz", "lol", "miao", "omg"};
        Random rand = new Random(0);
        List<String> keyedElements = rand.ints(0, strings.length)
                .limit(100)
                .mapToObj(i -> strings[i]).collect(Collectors.toList());

        DataStream<Integer> bcast = env.fromCollection(bcastedElements);
        DataStream<String> key = env.fromCollection(keyedElements);

        KeySelector<String, String> ks = k -> k;

        bcast = bcast.broadcast();
        key = key.keyBy(ks);

        key.connect(bcast)
                .map(new CoMapFn<>(ks)).setParallelism(8)
                .addSink(new AccumulatingSink()).setParallelism(1);

        JobExecutionResult jobResult = env.execute();
        HashMap<String, ValuesPerThread<String>> result = jobResult.getAccumulatorResult(AccumulatingSink.ACC_NAME);

        assertEquals(8, result.size());

        // broadcast are the same
        for (ValuesPerThread<String> values : result.values()) {
            List<Object> bcasted = values.getMap().get("bcast");
            assertThat(bcasted, is(bcastedElements));
        }

        Map<String, String> valuesByOperator = new HashMap<>();

        for (ValuesPerThread<String> values : result.values()) {
            String threadName = values.threadName;
            for (String k : values.getMap().keySet()) {
                if (!k.equals("bcast")) {
                    if (valuesByOperator.containsKey(k) &&
                            !valuesByOperator.get(k).equals(threadName)) {
                        throw new AssertionFailedError("The same key was managed by two different partitions: "
                                + k + ": " + valuesByOperator.get(k) + " <> " + threadName);
                    }

                    valuesByOperator.put(k, threadName);
                }
            }
        }
    }

    private static class CoMapFn<IN1, IN2> extends RichCoMapFunction<IN1, IN2, Tuple3<String, String, Object>> {
        private final KeySelector<IN1, String> keySelector;
        private String threadName;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int index = getRuntimeContext().getIndexOfThisSubtask();
            threadName = "coMap" + index;
        }

        public CoMapFn(KeySelector<IN1, String> keySelector) {
            this.keySelector = keySelector;
        }

        @Override
        public Tuple3<String, String, Object> map1(IN1 in1) throws Exception {
            String key = keySelector.getKey(in1);
            return Tuple3.of(threadName, key, in1);
        }

        @Override
        public Tuple3<String, String, Object> map2(IN2 in2) throws Exception {
            String key = "bcast";
            return Tuple3.of(threadName, key, in2);
        }
    }

    private static class AccumulatingSink extends RichSinkFunction<Tuple3<String, String, Object>> {
        private final MapAccumulator<String> accumulator = new MapAccumulator<>();
        public static final String ACC_NAME = "records";

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(ACC_NAME, accumulator);
        }

        @Override
        public void invoke(Tuple3<String, String, Object> tuple3) throws Exception {
            accumulator.add(tuple3);
        }
    }

    private static class MapAccumulator<K>
            implements Accumulator<Tuple3<String, K, Object>, HashMap<String, ValuesPerThread<K>>> {
        private final HashMap<String, ValuesPerThread<K>> entries = new HashMap<>();

        @Override
        public void add(Tuple3<String, K, Object> tuple3) {
            entries.computeIfAbsent(tuple3.f0, ValuesPerThread::new)
                    .add(tuple3.f1, tuple3.f2);
        }

        @Override
        public HashMap<String, ValuesPerThread<K>> getLocalValue() {
            return entries;
        }

        @Override
        public void resetLocal() {
            //
        }

        @Override
        public void merge(Accumulator<Tuple3<String, K, Object>, HashMap<String, ValuesPerThread<K>>> accumulator) {
            // par 1, does nothing
        }

        @Override
        public Accumulator<Tuple3<String, K, Object>, HashMap<String, ValuesPerThread<K>>> clone() {
            return this;
        }

    }

    private static class ValuesPerThread<K> implements Serializable {
        public final String threadName;
        private final HashMap<K, List<Object>> map = new HashMap<>();

        public ValuesPerThread(String threadName) {
            this.threadName = threadName;
        }

        private void add(K key, Object value) {
            map.computeIfAbsent(key, k -> new LinkedList<>()).add(value);
        }

        public HashMap<K, List<Object>> getMap() {
            return map;
        }

        @Override
        public String toString() {
            return map.toString();
        }
    }
}
