package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.common.PartitionOrBcastPartitioner;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryID;
import junit.framework.AssertionFailedError;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Either;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by affo on 27/07/17.
 */
public class PartitioningTest {

    private final String[] strings = {"foo", "bar", "buz", "lol", "miao", "omg"};

    private List<String> elements(int n) {
        Random rand = new Random(0);
        return rand.ints(0, strings.length)
                .limit(n)
                .mapToObj(i -> strings[i]).collect(Collectors.toList());
    }

    @Test
    public void testKeyByConnected() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> bcastedElements = Arrays.asList(1, 2, 3, 4, 5);
        DataStream<Integer> bcast = env.fromCollection(bcastedElements);
        List<String> keyedElements = elements(100);
        DataStream<String> key = env.fromCollection(keyedElements);

        KeySelector<String, String> ks = k -> k;

        bcast = bcast.broadcast();
        key = key.keyBy(ks);

        key.connect(bcast)
                .map(new CoMapFn<>(ks)).setParallelism(8)
                .addSink(new AccumulatingSink<>()).setParallelism(1);

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

    @Test
    public void testCustomPartitioningEquivalentToKeyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int parallelism = 8;

        DataStream<String> key = env.fromCollection(elements(100));
        KeySelector<String, String> ks = k -> k;

        DataStream<MyString> wrapped = key.map(MyString::new);
        PartitionOrBcastPartitioner.apply(wrapped)
                .map(ms -> ms.wrapped).setParallelism(parallelism).returns(String.class)
                .map(new MapFn<>(ks)).setParallelism(parallelism)
                .addSink(new AccumulatingSink<>("acc1")).setParallelism(1);

        key.keyBy(ks)
                .map(new MapFn<>(ks)).setParallelism(parallelism)
                .addSink(new AccumulatingSink<>("acc2")).setParallelism(1);

        JobExecutionResult jobResult = env.execute();
        HashMap<String, ValuesPerThread<String>> result1 = jobResult.getAccumulatorResult("acc1");
        HashMap<String, ValuesPerThread<String>> result2 = jobResult.getAccumulatorResult("acc2");

        assertThat(result1, is(result2));
    }

    @Test
    public void partitioningWithQueries() throws Exception {
        // fixtures
        Query singleKey = new Query("", new QueryID(0, 1L));
        singleKey.addKey(strings[0]);

        Query multiKey = new Query("", new QueryID(0, 2L));
        multiKey.addKey(strings[0]);
        multiKey.addKey(strings[1]);
        multiKey.addKey(strings[2]);
        multiKey.addKey(strings[3]);

        PredicateQuery<String> predicateQuery = new PredicateQuery<>("",
                new QueryID(0, 3L),
                new PredicateQuery.SelectAll<>());

        List<String> elements = elements(100);

        // testing
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SplitStream<Either<String, Query>> input = env
                .addSource(
                        new SourceFunction<Either<String, Query>>() {
                            @Override
                            public void run(SourceContext<Either<String, Query>> sourceContext) throws Exception {
                                for (String element : elements) {
                                    sourceContext.collect(Either.Left(element));
                                }

                                Thread.sleep(2000);

                                sourceContext.collect(Either.Right(singleKey));
                                sourceContext.collect(Either.Right(multiKey));
                                sourceContext.collect(Either.Right(predicateQuery));
                            }

                            @Override
                            public void cancel() {
                                // does nothing
                            }
                        }
                )
                .split(record -> Collections.singleton(String.valueOf(record.isLeft())));

        DataStream<String> elementsStream = input.select("true")
                .map(
                        new MapFunction<Either<String, Query>, String>() {
                            @Override
                            public String map(Either<String, Query> r) throws Exception {
                                return r.left();
                            }
                        }
                );
        DataStream<Query> queriesStream = input.select("false")
                .map(
                        new MapFunction<Either<String, Query>, Query>() {
                            @Override
                            public Query map(Either<String, Query> r) throws Exception {
                                return r.right();
                            }
                        }
                );

        int parallelism = 8;

        KeySelector<String, String> ks = k -> k;

        // partitioning
        elementsStream = elementsStream.keyBy(ks);
        queriesStream = PartitionOrBcastPartitioner.apply(queriesStream);

        ConnectedStreams<String, Query> connected = elementsStream.connect(queriesStream);

        SplitStream<Either<Tuple3<String, String, String>, Tuple3<String, QueryID, Query>>> resultStream = connected
                .map(new FakeState<>(ks)).setParallelism(parallelism)
                .split(record -> Collections.singleton(String.valueOf(record.isLeft())));

        DataStream<Tuple3<String, String, Object>> postElements = resultStream.select("true")
                .map(r -> {
                    Tuple3<String, String, String> left = r.left();
                    return Tuple3.of(left.f0, left.f1, (Object) left.f2);
                })
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class));
        DataStream<Tuple3<String, QueryID, Object>> postQueries = resultStream.select("false")
                .map(r -> {
                    Tuple3<String, QueryID, Query> right = r.right();
                    return Tuple3.of(right.f0, right.f1, (Object) right.f2); // to object
                })
                .returns(new TypeHint<Tuple3<String, QueryID, Object>>() {
                });

        postElements
                .addSink(new AccumulatingSink<>("elements")).setParallelism(1);
        postQueries
                .addSink(new AccumulatingSink<>("queries")).setParallelism(1);

        JobExecutionResult jobResult = env.execute();
        // contains every elements run by threadName, by key
        HashMap<String, ValuesPerThread<String>> elementsResult = jobResult
                .getAccumulatorResult("elements");
        // contains every query run by threadName by queryID
        HashMap<String, ValuesPerThread<QueryID>> queriesResult = jobResult
                .getAccumulatorResult("queries");

        // testing sizes...
        int resultSize = elementsResult.values()
                .stream()
                .flatMap(vpt -> vpt.getMap().values().stream())
                .mapToInt(List::size)
                .sum();
        int querySize = queriesResult.values()
                .stream()
                .flatMap(vpt -> vpt.getMap().values().stream())
                .mapToInt(List::size)
                .sum();

        assertEquals(100, resultSize);
        assertTrue(querySize >= 3);

        // testing keyBy on elements
        Map<String, String> elementPartitionMapping = new HashMap<>();
        for (ValuesPerThread<String> values : elementsResult.values()) {
            String threadName = values.threadName;
            for (String k : values.getMap().keySet()) {
                if (elementPartitionMapping.containsKey(k) &&
                        !elementPartitionMapping.get(k).equals(threadName)) {
                    throw new AssertionFailedError("The same key was managed by two different partitions: "
                            + k + ": " + elementPartitionMapping.get(k) + " <> " + threadName);
                }

                elementPartitionMapping.put(k, threadName);
            }
        }

        // ------------- testing queries

        // the partitions associated to the keys of the query
        Set<String> singleQueryExpectedPartitions = singleKey.getKeys().stream()
                .map(elementPartitionMapping::get).collect(Collectors.toSet());

        assertEquals(1, singleQueryExpectedPartitions.size());

        // for every partition, if that query was expected to be processed
        // by that one, that partition processed the query (and viceversa)
        for (ValuesPerThread<QueryID> values : queriesResult.values()) {
            String threadName = values.threadName;

            if (singleQueryExpectedPartitions.contains(threadName)) {
                assertTrue(values.getMap().keySet().contains(singleKey.getQueryID()));
                Query q = (Query) values.getMap().get(singleKey.getQueryID()).get(0);
                assertEquals(1, q.numberOfPartitions);
            } else {
                assertFalse(values.getMap().keySet().contains(singleKey.getQueryID()));
            }
        }

        // the same for the multi
        Set<String> multiQueryExpectedPartitions = multiKey.getKeys().stream()
                .map(elementPartitionMapping::get).collect(Collectors.toSet());

        assertTrue(multiQueryExpectedPartitions.size() > 1);

        // for every partition, if that query was expected to be processed
        // by that one, that partition processed the query (and viceversa)
        for (ValuesPerThread<QueryID> values : queriesResult.values()) {
            String threadName = values.threadName;

            if (multiQueryExpectedPartitions.contains(threadName)) {
                assertTrue(values.getMap().keySet().contains(multiKey.getQueryID()));
                Query q = (Query) values.getMap().get(multiKey.getQueryID()).get(0);
                assertEquals(multiQueryExpectedPartitions.size(), q.numberOfPartitions);
            } else {
                assertFalse(values.getMap().keySet().contains(multiKey.getQueryID()));
            }
        }

        // eventually, for predicate

        // every partition should have processed the predicate (bcast)
        for (ValuesPerThread<QueryID> values : queriesResult.values()) {
            assertTrue(values.getMap().keySet().contains(predicateQuery.getQueryID()));
            Query q = (Query) values.getMap().get(predicateQuery.getQueryID()).get(0);
            assertEquals(parallelism, q.numberOfPartitions);
        }
    }

    private static class CoMapFn<IN1, IN2> extends RichCoMapFunction<IN1, IN2, Tuple3<String, String, Object>> {
        protected final KeySelector<IN1, String> keySelector;
        protected String threadName;

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

    private static class FakeState<IN1> extends RichCoMapFunction<IN1, Query,
            Either<Tuple3<String, String, IN1>, Tuple3<String, QueryID, Query>>> {
        private final Map<String, IN1> state = new HashMap<>();
        protected final KeySelector<IN1, String> keySelector;
        protected String threadName;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int index = getRuntimeContext().getIndexOfThisSubtask();
            threadName = "fakeState" + index;
        }

        public FakeState(KeySelector<IN1, String> keySelector) {
            this.keySelector = keySelector;
        }

        @Override
        public Either<Tuple3<String, String, IN1>, Tuple3<String, QueryID, Query>> map1(IN1 in1) throws Exception {
            String key = keySelector.getKey(in1);
            state.put(key, in1);
            return Either.Left(Tuple3.of(threadName, key, in1));
        }

        @Override
        public Either<Tuple3<String, String, IN1>, Tuple3<String, QueryID, Query>> map2(Query query) throws Exception {
            return Either.Right(Tuple3.of(threadName, query.queryID, query));
        }
    }

    private static class MapFn<I> extends RichMapFunction<I, Tuple3<String, String, Object>> {
        private final KeySelector<I, String> keySelector;
        private String threadName;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int index = getRuntimeContext().getIndexOfThisSubtask();
            threadName = "map" + index;
        }

        public MapFn(KeySelector<I, String> keySelector) {
            this.keySelector = keySelector;
        }

        @Override
        public Tuple3<String, String, Object> map(I in) throws Exception {
            String key = keySelector.getKey(in);
            return Tuple3.of(threadName, key, in);
        }
    }

    private static class AccumulatingSink<K> extends RichSinkFunction<Tuple3<String, K, Object>> {
        private final MapAccumulator<K> accumulator = new MapAccumulator<>();
        public static final String ACC_NAME = "records";
        private final String accumulatorName;

        public AccumulatingSink() {
            this(ACC_NAME);
        }

        public AccumulatingSink(String accumulatorName) {
            this.accumulatorName = accumulatorName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(accumulatorName, accumulator);
        }

        @Override
        public void invoke(Tuple3<String, K, Object> tuple3) throws Exception {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ValuesPerThread<?> that = (ValuesPerThread<?>) o;

            if (threadName != null ? !threadName.equals(that.threadName) : that.threadName != null) return false;
            return map != null ? map.equals(that.map) : that.map == null;
        }

        @Override
        public int hashCode() {
            int result = threadName != null ? threadName.hashCode() : 0;
            result = 31 * result + (map != null ? map.hashCode() : 0);
            return result;
        }
    }

    private static class MyString implements PartitionOrBcastPartitioner.Partitionable<String>, Serializable {
        public final String wrapped;

        public MyString(String wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Set<String> getKeys() {
            return Collections.singleton(wrapped);
        }

        @Override
        public void setNumberOfPartitions(int n) {
            // does nothing
        }
    }
}
