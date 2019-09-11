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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Experiment description:
 * The pipeline sleeps (busy wait) for a configurable uniformly random number of milliseconds in the analytical part,
 * and then performs a random transaction that can be as heavy as the user specifies.
 * <p>
 * We track throughput and latency before and after the analytical and transactional part.
 * <p>
 * Every duration parameter is in milliseconds.
 */
public class NewMixed {
    public static final String A_TRACKER_SERVER = "a-tracker";
    public static final String T_TRACKER_SERVER = "t-tracker";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final int runtimeSeconds = parameters.getInt("runtimeSeconds", 180);
        final boolean analyticsOnly = parameters.getBoolean("analyticsOnly", false);
        final long aMaxSleep = parameters.getLong("aMaxSleep", 1000);
        final long tMaxSleep = parameters.getLong("tMaxSleep", 1000);
        final long aMinSleep = parameters.getLong("aMinSleep", 5);
        final long tMinSleep = parameters.getLong("tMinSleep", 5);
        final long windowSize = parameters.getLong("windowSize", 5);
        final long windowSlide = parameters.getLong("windowSlide", 5);
        final int transientSpan = parameters.getInt("transient", 10);
        final TransientPeriod transientPeriod = new TransientPeriod(transientSpan);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        SingleOutputStreamOperator<Event> events = env
                .addSource(new Random(runtimeSeconds, transientPeriod));
        events = config.addToSourcesSharingGroup(events, "Events");

        events.addSink(new ThroughputMeter<>("input-throughput", transientPeriod))
                .setParallelism(1).name("MeasureInputRate")
                .slotSharingGroup("default");

        events.addSink(new LatencyTrackerStart<>(A_TRACKER_SERVER, transientPeriod))
                .setParallelism(1).name("A-StartTracker")
                .slotSharingGroup("default");

        DataStream<Event> delayed = events;
        if (windowSize <= 0) {
            delayed = delayed
                    .map(new MapFunction<Event, Event>() {
                        BusyWaitSleeper sleeper = new BusyWaitSleeper(aMaxSleep, aMinSleep);

                        @Override
                        public Event map(Event event) throws Exception {
                            sleeper.sleep();
                            return event;
                        }
                    })
                    .name("AnalyticsSleep")
                    .slotSharingGroup("default");
        } else {
            delayed = delayed
                    .keyBy(new KeySelector<Event, Long>() {
                        @Override
                        public Long getKey(Event event) throws Exception {
                            return event.walTS;
                        }
                    })
                    .timeWindow(Time.milliseconds(windowSize), Time.milliseconds(windowSlide))
                    .apply(new WindowFunction<Event, Event, Long, TimeWindow>() {
                        BusyWaitSleeper sleeper = new BusyWaitSleeper(aMaxSleep, aMinSleep);

                        @Override
                        public void apply(Long k, TimeWindow w, Iterable<Event> content, Collector<Event> collector) throws Exception {
                            sleeper.sleep();
                            collector.collect(content.iterator().next());
                        }
                    })
                    .name("PostWindowAnalyticsSleep")
                    .slotSharingGroup("default");
        }

        delayed
                .addSink(
                        new ThroughputMeter<>("before-tgraph-throughput",
                                transientPeriod))
                .setParallelism(1).name("MeasureTGraphInputRate");

        delayed.addSink(new LatencyTrackerEnd<>(A_TRACKER_SERVER, "analytics-latency"))
                .name("A-EndTracker").setParallelism(1);

        // if only analytics the job is over
        if (!analyticsOnly) {
            // tracker to measure latency and throughput of the tgraph
            delayed.addSink(new LatencyTrackerStart<>(T_TRACKER_SERVER, transientPeriod))
                    .setParallelism(1).name("T-StartTracker");

            TStream<Event> opened = tEnv.open(delayed).opened;

            KeySelector<Event, String> ks = p -> p.walTS + "";
            TStream<Event> afterState = opened
                    .state("OWState", ks, new OverwriteState(tMaxSleep, tMinSleep), config.parallelism)
                    .leftUnchanged;

            DataStream<TransactionResult> results = tEnv.close(afterState);

            DataStream<Event> toID = results
                    .map(tr -> ((Event) tr.f2))
                    .name("ToID")
                    .returns(Event.class);

            toID.addSink(new LatencyTrackerEnd<>(T_TRACKER_SERVER, "tgraph-latency"))
                    .name("T-EndTracker").setParallelism(1);
            toID.addSink(
                    new ThroughputMeter<>("after-tgraph-throughput",
                            transientPeriod))
                    .setParallelism(1).name("MeasureTGraphOutputRate");
        }

        env.execute("Sleep Experiment");
    }

    private static class Random extends RichParallelSourceFunction<Event> {
        private boolean stop = false;
        private int runtimeSeconds;
        private IntCounter generatedRecords = new IntCounter();
        private transient JobControlClient jobControlClient;

        private TransientPeriod transientPeriod;

        public Random(int runtimeSeconds, TransientPeriod transientPeriod) {
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
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Timer timer = null;
            while (!stop) {
                Event e = new Event(getRuntimeContext().getIndexOfThisSubtask());
                sourceContext.collect(e);
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

    private static class Event implements Serializable, UniquelyRepresentableForTracking {
        public long walTS;
        public int taskID;

        public Event() {
        }

        public Event(int taskID) {
            this.walTS = System.currentTimeMillis();
            this.taskID = taskID;
        }

        @Override
        public String getUniqueRepresentation() {
            return taskID + "-" + walTS;
        }

        @Override
        public String toString() {
            return getUniqueRepresentation();
        }
    }

    private static class OverwriteState implements StateFunction<Event, Long> {
        private BusyWaitSleeper sleeper;

        public OverwriteState(long tMaxSleep, long tMinSleep) {
            sleeper = new BusyWaitSleeper(tMaxSleep, tMinSleep);
        }

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
        public void apply(Event element, ObjectHandler<Long> handler) {
            try {
                sleeper.sleep();
                handler.write(element.walTS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
