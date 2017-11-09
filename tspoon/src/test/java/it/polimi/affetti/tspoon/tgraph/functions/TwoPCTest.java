package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.test.CollectionSource;
import it.polimi.affetti.tspoon.test.ResultUtils;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.backed.Graph;
import it.polimi.affetti.tspoon.tgraph.backed.GraphOutput;
import it.polimi.affetti.tspoon.tgraph.twopc.BufferFunction;
import it.polimi.affetti.tspoon.tgraph.twopc.ReduceVotesFunction;
import it.polimi.affetti.tspoon.tgraph.twopc.StandardTransactionsIndex;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by affo on 27/07/17.
 */
public class TwoPCTest {
    @Test
    public void testReduceVotes() throws Exception {
        List<Integer> elements = Arrays.asList(2, 3, 4);
        Graph<Metadata> reduced = new Graph<Metadata>() {
            @Override
            protected GraphOutput<Metadata> doDraw(StreamExecutionEnvironment env) {
                env.setParallelism(1);

                DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
                DataStream<Metadata> meta = ds.flatMap(
                        new FlatMapFunction<Integer, Metadata>() {
                            int tid = 1;
                            Vote[] votes = {Vote.COMMIT, Vote.COMMIT, Vote.ABORT, Vote.REPLAY};

                            @Override
                            public void flatMap(Integer integer, Collector<Metadata> collector) throws Exception {
                                for (int i = 0; i < integer; i++) {
                                    Metadata m = new Metadata(tid);
                                    m.batchSize = integer;
                                    m.vote = votes[i];
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
                return new GraphOutput<>(out);
            }
        };


        GraphOutput<Metadata> graphOutput = reduced.draw();
        ResultUtils.addAccumulator(graphOutput.output, "out");
        JobExecutionResult result = reduced.execute();
        Set<Metadata> output = new HashSet<>(ResultUtils.extractResult(result, "out"));
        Set<Vote> expectedVotes = new HashSet<>(Arrays.asList(Vote.COMMIT, Vote.ABORT, Vote.REPLAY));
        Set<Vote> actualVotes = output.stream().map(m -> m.vote).collect(Collectors.toSet());

        assertEquals(3, output.size());
        assertEquals(expectedVotes, actualVotes);
    }

    @Test
    public void testBuffer() throws Exception {
        List<Integer> elements = Arrays.asList(2, 3, 2, 1);
        List<Metadata> metas = Arrays.asList(
                new Metadata(1, Vote.COMMIT, 0),
                new Metadata(2, Vote.ABORT, 0),
                new Metadata(3, Vote.REPLAY, 0),
                new Metadata(4, Vote.COMMIT, 0)
        );

        Graph<Enriched<Integer>> buffered = new Graph<Enriched<Integer>>() {
            @Override
            protected GraphOutput<Enriched<Integer>> doDraw(StreamExecutionEnvironment env) {
                env.setParallelism(1);

                DataStream<Integer> ds = env.addSource(new CollectionSource<>(elements)).returns(Integer.class);
                TStream<Integer> ts = OTStream.fromStream(ds, new StandardTransactionsIndex()).opened;
                DataStream<Enriched<Integer>> enriched = ts.flatMap(
                        e -> IntStream.range(0, e).boxed().collect(Collectors.toList())
                ).getEnclosingStream();

                DataStream<Metadata> meta = env.addSource(new CollectionSource<>(metas)).returns(Metadata.class);

                ConnectedStreams<Enriched<Integer>, Metadata> connected = enriched.connect(meta)
                        .keyBy(
                                new KeySelector<Enriched<Integer>, Integer>() {
                                    @Override
                                    public Integer getKey(Enriched<Integer> e) throws Exception {
                                        return e.metadata.timestamp;
                                    }
                                },
                                new KeySelector<Metadata, Integer>() {
                                    @Override
                                    public Integer getKey(Metadata metadata) throws Exception {
                                        return metadata.timestamp;
                                    }
                                }
                        );

                DataStream<Enriched<Integer>> out = connected.flatMap(new BufferFunction<>()).setParallelism(4);

                return new GraphOutput<>(out);
            }
        };


        GraphOutput<Enriched<Integer>> graphOutput = buffered.draw();
        ResultUtils.addAccumulator(graphOutput.output, "out");
        JobExecutionResult result = buffered.execute();
        List<Enriched<Integer>> output = ResultUtils.extractResult(result, "out");

        List<Tuple2<Integer, Vote>> votes = output.stream().map(e -> Tuple2.of(e.metadata.tid, e.metadata.vote))
                .collect(Collectors.toList());
        // computing by hand without group by
        Map<Integer, List<Vote>> actualVotes = new HashMap<>();
        votes.forEach(t -> {
            List<Vote> vs = actualVotes.computeIfAbsent(t.f0, (k) -> new LinkedList<>());
            vs.add(t.f1);
        });

        assertEquals(4, actualVotes.size());

        Map<Integer, Vote> expectedVotes = new HashMap<>();
        expectedVotes.put(1, Vote.COMMIT);
        expectedVotes.put(2, Vote.ABORT);
        expectedVotes.put(3, Vote.REPLAY);
        expectedVotes.put(4, Vote.COMMIT);

        for (Map.Entry<Integer, List<Vote>> entry : actualVotes.entrySet()) {
            for (Vote vote : entry.getValue()) {
                assertEquals(expectedVotes.get(entry.getKey()), vote);
            }
        }
    }
}
