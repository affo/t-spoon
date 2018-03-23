package it.polimi.affetti.tspoon.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Created by affo on 29/07/17.
 *
 * Sample wordcount to evaluate how much flink can go fast
 */
public class FlinkGoAsFastAsYouCan {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final long bufferTimeout = parameters.getLong("bufferTO", 0);
        final int par = parameters.getInt("par", 4);
        final int batchSize = parameters.getInt("batchSize", 10000);

        env.setBufferTimeout(bufferTimeout);
        env.setParallelism(par);

        DataStream<String> text = env.addSource(new LineSource(batchSize));

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .keyBy(0).sum(1);
        counts.print();

        env.execute("Need4Speed");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    public static final class LineSource extends RichParallelSourceFunction<String> {
        private int limit, globalLimit;
        private boolean stop = false;
        private Random random;
        public static final String[] LINES = {
                "Cras sollicitudin quam vitae eros rutrum accumsan Suspendisse potenti Fusce",
                "accumsan facilisis mauris eu cursus dolor convallis sed Duis consectetur sem",
                "id posuere consectetur libero tellus molestie mauris ac blandit sem quam in",
                "nisl Vivamus ac libero vitae elit lacinia tincidunt Proin id pellentesque",
                "sapien Phasellus consectetur tortor vitae velit ornare vel mattis diam",
                "vulputate Sed sagittis urna sapien sit amet vestibulum mi interdum sed Nulla",
                "accumsan urna vitae imperdiet semper Quisque at nulla nibh Maecenas eget massa",
                "est Pellentesque habitant morbi tristique senectus et netus et malesuada fames",
                "ac turpis egestas Cras placerat interdum elit et dictum Morbi posuere dui at",
                "varius lacinia dui sem mollis nisi ac eleifend tortor metus feugiat erat"
        };

        public LineSource(int globalLimit) {
            this.globalLimit = globalLimit;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int numberOfTasks = getRuntimeContext().getNumberOfParallelSubtasks();
            int taskId = getRuntimeContext().getIndexOfThisSubtask();

            this.limit = globalLimit / numberOfTasks;

            if (taskId == 0) {
                this.limit += globalLimit % numberOfTasks;
            }

            this.random = new Random(taskId);
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (!stop && limit > 0) {
                sourceContext.collect(LINES[random.nextInt(LINES.length)]);
                limit--;
            }
        }

        @Override
        public void cancel() {
            stop = true;
        }
    }
}
