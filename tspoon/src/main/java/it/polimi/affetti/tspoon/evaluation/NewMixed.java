package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Experiment description:
 * The pipeline sleeps for a configurable uniformly random number of milliseconds,
 * and then perform a random transaction (overwrite the random sleep to internal state).
 *
 * We track throughput and latency before and after the transactional part.
 */
public class NewMixed {
    public static final String TRACKER_SERVER = "tracker";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final int runtimeSeconds = parameters.getInt("runtimeSeconds", 180);
        final boolean analyticsOnly = parameters.getBoolean("analyticsOnly", false);
        final long maxSleep = parameters.getLong("maxSleep", 1000);
        final long minSleep = parameters.getLong("minSleep", 5);
        final int transientSpan = parameters.getInt("transient", 30);
        final TransientPeriod transientPeriod = new TransientPeriod(transientSpan);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        SingleOutputStreamOperator<Long> sleeps = env
                .addSource(new Random(minSleep, maxSleep, runtimeSeconds, transientPeriod));
        sleeps = config.addToSourcesSharingGroup(sleeps, "RandomVotes");

        sleeps.addSink(new ThroughputMeter<>("input-throughput", transientPeriod))
                .setParallelism(1).name("MeasureInputRate")
                .slotSharingGroup("default");

        DataStream<Long> delayed = sleeps.map(new Sleeper()).slotSharingGroup("default");

        delayed
                .addSink(
                        new ThroughputMeter<>("before-tgraph-throughput",
                                transientPeriod))
                .setParallelism(1).name("MeasureTGraphInputRate");

        // if only analytics the job is over
        if (!analyticsOnly) {
            DataStream<Point> points = delayed
                    .map(new RichMapFunction<Long, Point>() {
                        @Override
                        public Point map(Long v) throws Exception {
                            long walTS = System.currentTimeMillis();
                            int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                            return new Point(walTS, v, taskIndex);

                        }
                    })
                    .slotSharingGroup("default");

            // tracker to measure latency and throughput of the tgraph
            points.addSink(new LatencyTrackerStart<>(TRACKER_SERVER, transientPeriod))
                    .setParallelism(1).name("StartTracker");

            TStream<Point> opened = tEnv.open(points).opened;

            KeySelector<Point, String> ks = p -> p.value + "";
            TStream<Point> afterState = opened
                    .state("rndState", ks, new RndState(), config.parallelism)
                    .leftUnchanged;

            DataStream<TransactionResult> results = tEnv.close(afterState);

            DataStream<Point> toID = results
                    .map(tr -> ((Point) tr.f2))
                    .name("ToID")
                    .returns(Point.class);

            toID.addSink(new LatencyTrackerEnd<>(TRACKER_SERVER, "tgraph-latency"))
                    .name("EndTracker").setParallelism(1);
            toID.addSink(
                    new ThroughputMeter<>("after-tgraph-throughput",
                            transientPeriod))
                    .setParallelism(1).name("MeasureTGraphOutputRate");
        }

        env.execute("Sleep Experiment");
    }

    private static class Random extends RichParallelSourceFunction<Long> {
        private final long maxSleep;
        private final long minSleep;
        private boolean stop = false;
        private int runtimeSeconds;
        private IntCounter generatedRecords = new IntCounter();
        private transient JobControlClient jobControlClient;

        private TransientPeriod transientPeriod;

        public Random(long minSleep, long maxSleep, int runtimeSeconds, TransientPeriod transientPeriod) {
            this.maxSleep = maxSleep;
            this.minSleep = minSleep;
            this.runtimeSeconds = runtimeSeconds;
            this.transientPeriod = transientPeriod;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("collected-records", generatedRecords);

            ParameterTool parameterTool = (ParameterTool)
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            jobControlClient = JobControlClient.get(parameterTool);

            transientPeriod.start();
        }

        @Override
        public void close() throws Exception {
            super.close();
            jobControlClient.close();
        }

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            Timer timer = null;
            while (!stop) {
                long rnd = (long) (Math.random() * maxSleep) + minSleep;
                sourceContext.collect(rnd);
                if (transientPeriod.hasFinished()) {
                    generatedRecords.add(1);
                    if (timer == null) {
                        // start the elapsedTime for the job
                        timer = new Timer();
                        timer.schedule(new TimerTask() {
                            @Override
                            public void run() {
                                stop = true;
                            }
                        }, runtimeSeconds * 1000);
                    }
                }
            }

            timer.cancel();
            timer.purge();
            jobControlClient.publishFinishMessage();
        }

        @Override
        public void cancel() {
            stop = true;
        }
    }

    private static class Sleeper implements MapFunction<Long, Long> {
        @Override
        public Long map(Long sleepTime) throws Exception {
            long currentTime = System.nanoTime();
            // busy wait
            while (System.nanoTime() - currentTime < sleepTime * 1000) ;
            return sleepTime;
        }
    }

    private static class Point implements Serializable, UniquelyRepresentableForTracking {
        public long walTS, value;
        public int taskID;

        public Point() {
        }

        public Point(long walTS, long value, int taskID) {
            this.walTS = walTS;
            this.value = value;
            this.taskID = taskID;
        }

        @Override
        public String getUniqueRepresentation() {
            return taskID + "-" + walTS + "-" + value;
        }

        @Override
        public String toString() {
            return getUniqueRepresentation();
        }
    }

    private static class RndState implements StateFunction<Point, Long> {

        @Override
        public Long defaultValue() {
            return 0L;
        }

        @Override
        public Long copyValue(Long value) {
            return new Long(value);
        }

        @Override
        public boolean invariant(Long value) {
            return true;
        }

        @Override
        public void apply(Point element, ObjectHandler<Long> handler) {
            handler.write(element.value);
        }
    }
}
