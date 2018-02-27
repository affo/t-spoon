package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.test.CollectionSource;
import it.polimi.affetti.tspoon.test.ResultUtils;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.twopc.ReduceVotesFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 27/07/17.
 */
public class FunctionsTest {
    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        AbstractTStream.setTransactionEnvironment(TransactionEnvironment.get(env));
    }

    @Test
    public void testMap() throws Exception {
        List<Integer> elements = Arrays.asList(1, 2, 3, 4, 5);

        DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
        TStream<Integer> ts = OTStream.fromStream(ds, null, 0).opened;
        DataStream<Enriched<Integer>> out = ts.map((MapFunction<Integer, Integer>) i -> i * 2).getEnclosingStream();

        ResultUtils.addAccumulator(out, "out");
        JobExecutionResult result = env.execute();
        List<Enriched<Integer>> output = ResultUtils.extractResult(result, "out");

        List<Integer> expectedOutput = Arrays.asList(2, 4, 6, 8, 10);
        List<Integer> expectedIds = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> expectedBatchSizes = Arrays.asList(1, 1, 1, 1, 1);

        assertEquals(expectedOutput, output.stream().map(e -> e.value).collect(Collectors.toList()));
        assertEquals(expectedIds, output.stream().map(e -> e.metadata.tid).collect(Collectors.toList()));
        assertEquals(expectedBatchSizes, output.stream().map(e -> e.metadata.getLastStepBatchSize())
                .collect(Collectors.toList()));
    }

    @Test
    public void testFlatMap() throws Exception {
        List<Integer> elements = Arrays.asList(2, 3, 2, 1);

        DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
        TStream<Integer> ts = OTStream.fromStream(ds, null, 0).opened;
        DataStream<Enriched<Integer>> out = ts.flatMap(
                e -> IntStream.range(0, e).boxed().collect(Collectors.toList())
        ).getEnclosingStream();

        ResultUtils.addAccumulator(out, "out");
        JobExecutionResult result = env.execute();
        List<Enriched<Integer>> output = ResultUtils.extractResult(result, "out");

        List<Integer> expectedOutput = Arrays.asList(
                0, 1,
                0, 1, 2,
                0, 1,
                0
        );
        List<Integer> expectedIds = Arrays.asList(1, 1, 2, 2, 2, 3, 3, 4);
        List<Integer> expectedBatchSizes = Arrays.asList(2, 2, 3, 3, 3, 2, 2, 1);

        assertEquals(expectedOutput, output.stream().map(e -> e.value).collect(Collectors.toList()));
        assertEquals(expectedIds, output.stream().map(e -> e.metadata.tid).collect(Collectors.toList()));
        assertEquals(expectedBatchSizes, output.stream().map(e -> e.metadata.getLastStepBatchSize())
                .collect(Collectors.toList()));
    }

    @Test
    public void testFilter() throws Exception {
        List<Integer> elements = IntStream.range(1, 11).boxed().collect(Collectors.toList());

        DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
        TStream<Integer> ts = OTStream.fromStream(ds, null, 0).opened;
        DataStream<Enriched<Integer>> out = ts.filter(e -> e % 2 == 0).getEnclosingStream();

        ResultUtils.addAccumulator(out, "out");
        JobExecutionResult result = env.execute();
        List<Enriched<Integer>> output = ResultUtils.extractResult(result, "out");

        List<Integer> expectedOutput = Arrays.asList(null, 2, null, 4, null, 6, null, 8, null, 10);
        List<Integer> expectedIds = elements;
        List<Integer> expectedBatchSizes = elements.stream().map(e -> 1).collect(Collectors.toList());

        assertEquals(expectedOutput, output.stream().map(e -> e.value).collect(Collectors.toList()));
        assertEquals(expectedIds, output.stream().map(e -> e.metadata.tid).collect(Collectors.toList()));
        assertEquals(expectedBatchSizes, output.stream().map(e -> e.metadata.getLastStepBatchSize())
                .collect(Collectors.toList()));
    }

    @Test
    public void testReduceVotes() throws Exception {
        DataStream<Integer> ds = env.addSource(new CollectionSource<>(Arrays.asList(2, 3, 4))).returns(Integer.class);
        DataStream<Metadata> meta = ds.flatMap(
                new FlatMapFunction<Integer, Metadata>() {
                    int tid = 1;
                    Vote[] votes = {Vote.COMMIT, Vote.COMMIT, Vote.ABORT, Vote.REPLAY};

                    @Override
                    public void flatMap(Integer integer, Collector<Metadata> collector) throws Exception {
                        Metadata metadata = new Metadata(tid);
                        int i = 0;
                        for (Metadata m : metadata.newStep(integer)) {
                            m.vote = votes[i];
                            i++;
                            collector.collect(m);
                        }
                        tid++;
                    }
                })
                .returns(Metadata.class);

        DataStream<Metadata> out = meta
                .keyBy(m -> m.timestamp)
                .flatMap(new ReduceVotesFunction())
                .setParallelism(4);


        ResultUtils.addAccumulator(out, "out");
        JobExecutionResult result = env.execute();
        Set<Metadata> output = new HashSet<>(ResultUtils.extractResult(result, "out"));
        Set<Vote> expectedVotes = new HashSet<>(Arrays.asList(Vote.COMMIT, Vote.ABORT, Vote.REPLAY));
        Set<Vote> actualVotes = output.stream().map(m -> m.vote).collect(Collectors.toSet());

        assertEquals(3, output.size());
        assertEquals(expectedVotes, actualVotes);
    }
}
