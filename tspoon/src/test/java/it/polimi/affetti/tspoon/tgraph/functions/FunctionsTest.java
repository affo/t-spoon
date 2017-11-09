package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.test.CollectionSource;
import it.polimi.affetti.tspoon.test.ResultUtils;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.OTStream;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.backed.Graph;
import it.polimi.affetti.tspoon.tgraph.backed.GraphOutput;
import it.polimi.affetti.tspoon.tgraph.twopc.StandardTransactionsIndex;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 27/07/17.
 */
public class FunctionsTest {
    @Test
    public void testMap() throws Exception {
        List<Integer> elements = Arrays.asList(1, 2, 3, 4, 5);
        Graph<Enriched<Integer>> map = new Graph<Enriched<Integer>>() {
            @Override
            protected GraphOutput<Enriched<Integer>> doDraw(StreamExecutionEnvironment env) {
                env.setParallelism(1);

                DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
                TStream<Integer> ts = OTStream.fromStream(ds, new StandardTransactionsIndex()).opened;
                DataStream<Enriched<Integer>> out = ts.map((MapFunction<Integer, Integer>) i -> i * 2).getEnclosingStream();
                return new GraphOutput<>(out);
            }
        };


        GraphOutput<Enriched<Integer>> graphOutput = map.draw();
        ResultUtils.addAccumulator(graphOutput.output, "out");
        JobExecutionResult result = map.execute();
        List<Enriched<Integer>> output = ResultUtils.extractResult(result, "out");

        List<Integer> expectedOutput = Arrays.asList(2, 4, 6, 8, 10);
        List<Integer> expectedIds = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> expectedBatchSizes = Arrays.asList(1, 1, 1, 1, 1);

        assertEquals(expectedOutput, output.stream().map(e -> e.value).collect(Collectors.toList()));
        assertEquals(expectedIds, output.stream().map(e -> e.metadata.tid).collect(Collectors.toList()));
        assertEquals(expectedBatchSizes, output.stream().map(e -> e.metadata.batchSize).collect(Collectors.toList()));
    }

    @Test
    public void testFlatMap() throws Exception {
        List<Integer> elements = Arrays.asList(2, 3, 2, 1);
        Graph<Enriched<Integer>> map = new Graph<Enriched<Integer>>() {
            @Override
            protected GraphOutput<Enriched<Integer>> doDraw(StreamExecutionEnvironment env) {
                env.setParallelism(1);

                DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
                TStream<Integer> ts = OTStream.fromStream(ds, new StandardTransactionsIndex()).opened;
                DataStream<Enriched<Integer>> out = ts.flatMap(
                        e -> IntStream.range(0, e).boxed().collect(Collectors.toList())
                ).getEnclosingStream();
                return new GraphOutput<>(out);
            }
        };


        GraphOutput<Enriched<Integer>> graphOutput = map.draw();
        ResultUtils.addAccumulator(graphOutput.output, "out");
        JobExecutionResult result = map.execute();
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
        assertEquals(expectedBatchSizes, output.stream().map(e -> e.metadata.batchSize).collect(Collectors.toList()));
    }

    @Test
    public void testFilter() throws Exception {
        List<Integer> elements = IntStream.range(1, 11).boxed().collect(Collectors.toList());
        Graph<Enriched<Integer>> map = new Graph<Enriched<Integer>>() {
            @Override
            protected GraphOutput<Enriched<Integer>> doDraw(StreamExecutionEnvironment env) {
                env.setParallelism(1);

                DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
                TStream<Integer> ts = OTStream.fromStream(ds, new StandardTransactionsIndex()).opened;
                DataStream<Enriched<Integer>> out = ts.filter(e -> e % 2 == 0).getEnclosingStream();
                return new GraphOutput<>(out);
            }
        };


        GraphOutput<Enriched<Integer>> graphOutput = map.draw();
        ResultUtils.addAccumulator(graphOutput.output, "out");
        JobExecutionResult result = map.execute();
        List<Enriched<Integer>> output = ResultUtils.extractResult(result, "out");

        List<Integer> expectedOutput = Arrays.asList(null, 2, null, 4, null, 6, null, 8, null, 10);
        List<Integer> expectedIds = elements;
        List<Integer> expectedBatchSizes = elements.stream().map(e -> 1).collect(Collectors.toList());

        assertEquals(expectedOutput, output.stream().map(e -> e.value).collect(Collectors.toList()));
        assertEquals(expectedIds, output.stream().map(e -> e.metadata.tid).collect(Collectors.toList()));
        assertEquals(expectedBatchSizes, output.stream().map(e -> e.metadata.batchSize).collect(Collectors.toList()));
    }
}
