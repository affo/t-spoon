package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.ComposedID;
import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.runtime.JobControlClient;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 * It's important to tune the slide in order to make the computation for the tgraph less heavy
 * than the analytical part.
 * Every closed window outputs ~20 records (10 top + 10 bottom). If the slide is 1ms, you will get a
 * tgraph input-rate of around 2000r/s (* number of windows closed = number of areas)
 * and, probably, the source will be back-pressured because the
 * topK window can't cope with the slide (it needs maybe 3 ms to get processed, which is 3 times the slide).
 * You need a slide wide enough to allow the source to go faster than the window throughput.
 */
public class Mixed {
    public static final String TRACKER_SERVER = "tracker";
    public static final int numberOfParticipants = 100000;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final int runtimeSeconds = parameters.getInt("runtimeSeconds", 180);
        final int windowSizeSeconds = parameters.getInt("windowSizeSeconds");
        final int windowSlideMilliseconds = parameters.getInt("windowSlideMilliseconds");

        final boolean analyticsOnly = parameters.getBoolean("analyticsOnly", false);
        final boolean enableDelayer = parameters.getBoolean("enableDelayer", false);
        final boolean enableAnomalyDetector = parameters.getBoolean("enableAnomaly", false);
        final int anomalyThreshold = parameters.getInt("anomalyThreshold", 10000);
        final int analyticsPar = parameters.getInt("analyticsPar", config.parallelism);
        final int numberOfAreas = parameters.getInt("areas", 100);
        final int transientSpan = parameters.getInt("transient", 30);
        final TransientPeriod transientPeriod = new TransientPeriod(transientSpan);

        final Time fixedSlide = Time.milliseconds(windowSlideMilliseconds);
        final Time anomalyDetectionWindowSize = Time.seconds(10);
        final Time topKWindowSize = Time.seconds(windowSizeSeconds);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        // about parallelism
        int tgPar = analyticsPar;
        String analyticGroup = "default";

        if (analyticsPar < config.parallelism) {
            //separate into 2 slotSharing groups
            analyticGroup = "analytic";
            tgPar = config.parallelism - analyticsPar;
            env.setParallelism(tgPar);
        }

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        SingleOutputStreamOperator<Vote> votes = env
                .addSource(new VotesSource(runtimeSeconds, numberOfAreas, transientPeriod));
        votes = config.addToSourcesSharingGroup(votes, "RandomVotes");

        votes.addSink(new ThroughputMeter<>("input-throughput", transientPeriod))
                .setParallelism(1).name("MeasureInputRate")
                .slotSharingGroup("default");

        if (enableAnomalyDetector) {
            votes = votes
                    .keyBy(v -> v.geographicArea)
                    .timeWindow(anomalyDetectionWindowSize) // tumbling window
                    .apply(new AnomalyDetection(anomalyThreshold))
                    .name("AnomalyDetection - " + anomalyThreshold)
                    .setParallelism(analyticsPar)
                    .slotSharingGroup(analyticGroup);
        }

        DataStream<Tuple3<String, String, Boolean>> topBottom = votes
                .keyBy(v -> v.geographicArea)
                .timeWindow(topKWindowSize, fixedSlide)
                .apply(new TopK(10))
                .name("Top10")
                .setParallelism(analyticsPar)
                .slotSharingGroup(analyticGroup);

        if (enableDelayer) {
            // The Delayer distributes the records and avoid bursty input to the tgraph
            topBottom = topBottom.flatMap(new Delayer(fixedSlide.toMilliseconds(), numberOfAreas))
                    .setParallelism(analyticsPar)
                    .slotSharingGroup(analyticGroup)
                    .name("Delayer");
        }

        topBottom
                .addSink(
                        new ThroughputMeter<>("before-tgraph-throughput",
                                transientPeriod))
                .setParallelism(1).name("MeasureTGraphInputRate");

        // if only analytics the job is over
        if (!analyticsOnly) {
            DataStream<Point> points = topBottom.map(new ToPoint())
                    .slotSharingGroup("default");

            // tracker to measure latency and throughput of the tgraph
            points.addSink(new LatencyTrackerStart<>(TRACKER_SERVER, transientPeriod))
                    .setParallelism(1).name("StartTracker");

            TStream<Point> opened = tEnv.open(points).opened;

            KeySelector<Point, String> ks = p -> p.assignee; // key by participant
            TStream<Point> afterState = opened
                    .state("votes", ks, new VotesState(), tgPar)
                    .leftUnchanged;

            DataStream<TransactionResult> results = tEnv.close(afterState);

            results
                    .map(tr -> ((Point) tr.f2).id)
                    .name("ToID")
                    .returns(ComposedID.class)
                    .addSink(new LatencyTrackerEnd<>(TRACKER_SERVER, "tgraph-latency"))
                    .name("EndTracker").setParallelism(1);
        }

        env.execute("Voting Experiment");
    }

    public static class Vote implements Serializable {
        public ComposedID id;
        public String participant;
        public String geographicArea;

        public Vote() {
        }

        public Vote(ComposedID id, String participant, String area) {
            this.id = id;
            this.participant = participant;
            this.geographicArea = area;
        }

        @Override
        public String toString() {
            return "Vote{" +
                    "id=" + id +
                    ", participant='" + participant + '\'' +
                    ", geographicArea='" + geographicArea + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Vote vote = (Vote) o;

            return id != null ? id.equals(vote.id) : vote.id == null;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    public static class Point implements Serializable, UniquelyRepresentableForTracking {
        public ComposedID id;
        public String assignee;
        public boolean positive;

        public Point() {
        }

        public Point(ComposedID id, String assignee, boolean positive) {
            this.id = id;
            this.assignee = assignee;
            this.positive = positive;
        }

        @Override
        public String getUniqueRepresentation() {
            return id.getUniqueRepresentation();
        }

        @Override
        public String toString() {
            return getUniqueRepresentation() + (positive ? "+" : "-") + assignee;
        }
    }

    private static class ToPoint extends RichMapFunction<Tuple3<String, String, Boolean>, Point> {
        private int sourceID, count;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sourceID = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public Point map(Tuple3<String, String, Boolean> tup) throws Exception {
            return new Point(ComposedID.of(sourceID, count++), tup.f1, tup.f2);
        }
    }

    private static class VotesState implements StateFunction<Point, Integer> {

        @Override
        public Integer defaultValue() {
            return 0;
        }

        @Override
        public Integer copyValue(Integer value) {
            return new Integer(value);
        }

        @Override
        public boolean invariant(Integer votes) {
            return votes >= 0 && votes <= 10;
        }

        @Override
        public void apply(Point element, ObjectHandler<Integer> handler) {
            Integer numberOfVotes = handler.read();
            if (element.positive) {
                numberOfVotes++;
            } else {
                numberOfVotes--;
            }
            handler.write(numberOfVotes);
        }
    }

    private static class VotesSource extends RichParallelSourceFunction<Vote> {
        private boolean stop = false;
        private transient VoteGenerator generator;
        private int runtimeSeconds;
        private int areas;
        private IntCounter generatedRecords = new IntCounter();
        private transient JobControlClient jobControlClient;

        private TransientPeriod transientPeriod;

        public VotesSource(int runtimeSeconds, int areas, TransientPeriod transientPeriod) {
            this.runtimeSeconds = runtimeSeconds;
            this.areas = areas;
            this.transientPeriod = transientPeriod;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            generator = new VoteGenerator(getRuntimeContext().getIndexOfThisSubtask(), areas);

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
        public void run(SourceContext<Vote> sourceContext) throws Exception {
            Timer timer = null;
            while (!stop) {
                sourceContext.collect(generator.generate());
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

    private static class VoteGenerator {
        private final int seed;
        private final Random random;
        private int areas;
        private int count = 0;

        public VoteGenerator(int seed, int areas) {
            this.seed = seed;
            this.random = new Random(seed);
            this.areas = areas;
        }

        private int index(double mean, double stddev, double upperLimit) {
            double g = random.nextGaussian() * stddev + mean;
            if (g > upperLimit - 1) {
                g = upperLimit - 1;
            }
            return (int) Math.round(Math.abs(g));
        }

        public Vote generate() {
            // int participantNumber = index(0.0, numberOfParticipants * 0.5, numberOfParticipants);
            int participantNumber = random.nextInt(numberOfParticipants); // uniform
            String participant = "p" + participantNumber;
            int mostProbableArea = participantNumber % areas;
            //int areaNumber = index(mostProbableArea, 2.0, numberOfAreas);
            int areaNumber = mostProbableArea; // uniform
            String area = "a" + areaNumber;
            return new Vote(ComposedID.of(seed, count++), participant, area);
        }
    }

    private static class AnomalyDetection implements WindowFunction<Vote, Vote, String, TimeWindow> {
        private final int threshold;

        public AnomalyDetection(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void apply(String area, TimeWindow timeWindow, Iterable<Vote> content,
                          Collector<Vote> collector) throws Exception {
            int count = 0;

            for (Vote vote : content) {
                count++;
            }

            if (count <= threshold) {
                for (Vote vote : content) {
                    collector.collect(vote);
                }
            }
        }
    }

    private static class TopK extends RichWindowFunction<Vote, Tuple3<String, String, Boolean>, String, TimeWindow> {
        private final int k;
        private final MetricAccumulator windowProcessingTime = new MetricAccumulator();

        public TopK(int k) {
            this.k = k;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("window-processing-time", windowProcessingTime);
        }

        @Override
        public void apply(String area, TimeWindow timeWindow,
                          Iterable<Vote> content, Collector<Tuple3<String, String, Boolean>> collector) throws Exception {
            long start = System.nanoTime();

            final HashMap<String, Integer> votesCount = new HashMap<>();

            for (Vote element : content) {
                int counter = votesCount.getOrDefault(element.participant, 0);
                votesCount.put(element.participant, ++counter);
            }

            ArrayList<Map.Entry<String, Integer>> ranking = new ArrayList<>(votesCount.entrySet());
            ranking.sort(Comparator.comparingInt(Map.Entry::getValue));

            int head = k > ranking.size() ? ranking.size() : k;
            int tail = ranking.size() - head >= k ? ranking.size() - k : head;
            List<Map.Entry<String, Integer>> topK = ranking.subList(0, head);
            List<Map.Entry<String, Integer>> bottomK = ranking.subList(tail, ranking.size());

            for (Map.Entry<String, Integer> entry : topK) {
                collector.collect(Tuple3.of(area, entry.getKey(), true));
            }

            for (Map.Entry<String, Integer> entry : bottomK) {
                collector.collect(Tuple3.of(area, entry.getKey(), false));
            }

            double procTime = (System.nanoTime() - start) / Math.pow(10, 6); // ms
            windowProcessingTime.add(procTime);
        }
    }

    private static class Delayer extends RichFlatMapFunction<Tuple3<String, String, Boolean>, Tuple3<String, String, Boolean>> {
        private final double timeSlice;
        private transient Timer timer;

        public Delayer(long timeDelta, int areas) {
            this.timeSlice = ((double) timeDelta) / areas;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.timer = new Timer();
        }

        @Override
        public void close() throws Exception {
            super.close();
            timer.cancel();
            timer.purge();
        }

        @Override
        public void flatMap(Tuple3<String, String, Boolean> t, Collector<Tuple3<String, String, Boolean>> collector) throws Exception {
            int areaIndex = Integer.parseInt(t.f0.substring(1));
            long delay = Math.round(timeSlice * areaIndex);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    collector.collect(t);
                }
            }, delay);
        }
    }
}
