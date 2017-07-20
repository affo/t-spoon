package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.query.QuerySource;
import it.polimi.affetti.tspoon.tgraph.query.QueryTuple;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Random;

public class TGraphTestDrive {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(0);
        Transactions tEnv = new Transactions(Strategy.OPTIMISTIC, IsolationLevel.PL0);

        // get input data
        DataStream<Tuple2<String, Integer>> kv = env
                .generateSequence(0, 100)
                .map(
                        new MapFunction<Long, Tuple2<String, Integer>>() {
                            final String[] keys = {"foo", "bar", "buz"};
                            final Random rand = new Random(0);

                            @Override
                            public Tuple2<String, Integer> map(Long aLong) throws Exception {
                                // TODO job control server
                                if (aLong == 100L) {
                                    Thread.sleep(2000);
                                }
                                return Tuple2.of(keys[rand.nextInt(keys.length)], rand.nextInt(100));
                            }
                        }).setParallelism(1);

        DataStream<QueryTuple> queries = env.addSource(new QuerySource());

        TStream<Tuple2<String, Integer>> open = tEnv.open(kv);

        StateStream<Tuple2<String, Integer>, Integer> balances = open.state(
                "balances", new OutputTag<Update<Integer>>("balances") {
                }, t -> t.f0,
                new StateFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer defaultValue() {
                        return 0;
                    }

                    @Override
                    public Integer copyValue(Integer value) {
                        return new Integer(value);
                    }

                    @Override
                    public boolean invariant(Integer value) {
                        return value > 0;
                    }

                    @Override
                    public void apply(Tuple2<String, Integer> element, ObjectHandler<Integer> handler) {
                        // this is the transaction:
                        // r(x) w(x)
                        handler.write(handler.read() + element.f1);
                    }
                }, queries, 8);

        balances.updates.print();

        tEnv.close(balances.leftUnchanged).forEach(DataStream::print);

        env.execute();
    }
}
