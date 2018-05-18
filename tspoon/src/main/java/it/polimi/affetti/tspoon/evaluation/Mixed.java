package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.common.ComposedID;
import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import it.polimi.affetti.tspoon.metrics.TimeDelta;
import it.polimi.affetti.tspoon.runtime.*;
import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
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
 * tgraph input-rate of around 2000r/s and, probably, the source will be back-pressured because the
 * topK window can't cope with the slide (it needs maybe 3 ms to get processed, which is 3 times the slide).
 * You need a slide wide enough to allow the source to go faster than the window throughput.
 */
public class Mixed {
    public static final String TRACKER_SERVER = "tracker";
    public static final int numberOfParticipants = 100000;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        final int numRecords = parameters.getInt("nRec", 500000);
        final int windowSizeSeconds = parameters.getInt("windowSizeSeconds");
        final int windowSlideMilliseconds = parameters.getInt("windowSlideMilliseconds");

        final boolean analyticsOnly = parameters.getBoolean("analyticsOnly", false);
        final boolean enableAnomalyDetector = parameters.getBoolean("enableAnomaly", false);
        final int anomalyThreshold = parameters.getInt("anomalyThreshold", 10000);
        final int analyticsPar = parameters.getInt("analyticsPar", config.parallelism);
        final int numberOfAreas = parameters.getInt("areas", 100);

        final Time fixedSlide = Time.milliseconds(windowSlideMilliseconds);
        final Time anomalyDetectionWindowSize = Time.seconds(10);
        final Time topKWindowSize = Time.seconds(windowSizeSeconds);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        TransactionEnvironment tEnv = TransactionEnvironment.fromConfig(config);

        SingleOutputStreamOperator<Vote> votes = env.addSource(new VotesSource(numRecords, numberOfAreas));
        votes = config.addToSourcesSharingGroup(votes, "RandomVotes");

        votes.addSink(new MeasureThroughput<>("input"))
                .setParallelism(1).name("MeasureInputRate")
                .slotSharingGroup("default");

        if (enableAnomalyDetector) {
            votes = votes
                    .keyBy(v -> v.geographicArea)
                    .timeWindow(anomalyDetectionWindowSize) // tumbling window
                    .apply(new AnomalyDetection(anomalyThreshold))
                    .name("AnomalyDetection - " + anomalyThreshold)
                    .setParallelism(analyticsPar)
                    .slotSharingGroup("default");
        }

        DataStream<Tuple3<String, String, Boolean>> topBottom = votes
                .keyBy(v -> v.geographicArea)
                .timeWindow(topKWindowSize, fixedSlide)
                .apply(new TopK(10))
                .setParallelism(analyticsPar)
                .name("Top10")
                .slotSharingGroup("default");

        topBottom = topBottom.flatMap(new Delayer(fixedSlide.toMilliseconds(), numberOfAreas))
                .setParallelism(analyticsPar).name("Delayer");

        topBottom.addSink(new MeasureThroughput<>("before-tgraph"))
                .setParallelism(1).name("MeasureTGraphInputRate");

        // if only analytics the job is over
        if (!analyticsOnly) {
            DataStream<Point> points = topBottom.map(new ToPoint())
                    .slotSharingGroup("default");

            // tracker to measure latency and throughput of the tgraph
            points.addSink(new TrackStart<>(TRACKER_SERVER))
                    .setParallelism(1).name("StartTracker");

            TStream<Point> opened = tEnv.open(points).opened;

            KeySelector<Point, String> ks = p -> p.assignee; // key by participant
            TStream<Point> afterState = opened
                    .state("votes", ks, new VotesState(), config.partitioning)
                    .leftUnchanged;

            DataStream<TransactionResult> results = tEnv.close(afterState);

            results
                    .map(tr -> ((Point) tr.f2).id)
                    .name("ToID")
                    .returns(ComposedID.class)
                    .addSink(new LatencyTracker<>(TRACKER_SERVER))
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
        private int numberOfRecords;
        private int areas;
        private transient JobControlClient jobControlClient;

        private transient TransientPeriod transientPeriod;

        public VotesSource(int numberOfRecords, int areas) {
            this.numberOfRecords = numberOfRecords;
            this.areas = areas;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            generator = new VoteGenerator(getRuntimeContext().getIndexOfThisSubtask(), areas);

            int realNumberOfRecords = numberOfRecords / getRuntimeContext().getNumberOfParallelSubtasks();
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                realNumberOfRecords += numberOfRecords % getRuntimeContext().getNumberOfParallelSubtasks();
            }
            this.numberOfRecords = realNumberOfRecords;

            ParameterTool parameterTool = (ParameterTool)
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            jobControlClient = JobControlClient.get(parameterTool);


            transientPeriod = new TransientPeriod();
            transientPeriod.start(parameterTool);
        }

        @Override
        public void close() throws Exception {
            super.close();
            jobControlClient.close();
        }

        @Override
        public void run(SourceContext<Vote> sourceContext) throws Exception {
            int count = 0;
            while (!stop && count < numberOfRecords) {
                sourceContext.collect(generator.generate());
                if (transientPeriod.hasFinished()) {
                    count++;
                }
            }

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
        private final long timeDelta;
        private final double timeSlice;
        private transient Timer timer;

        public Delayer(long timeDelta, int areas) {
            this.timeDelta = timeDelta;
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

    private static class MeasureThroughput<T> extends RichSinkFunction<T> {
        private int count = 0;
        private long start = -1;
        private final String label;

        private transient TransientPeriod transientPeriod;
        private MetricAccumulator throughput = new MetricAccumulator();

        public MeasureThroughput(String label) {
            this.label = label;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(label + "-throughput", throughput);

            transientPeriod = new TransientPeriod();
            transientPeriod.start((ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        }

        @Override
        public void invoke(T element) throws Exception {
            if (!transientPeriod.hasFinished()) {
                return;
            }

            count++;

            if (start < 0) {
                start = System.nanoTime();
            }

            long end = System.nanoTime();
            long delta = end - start;
            if (delta >= 10 * Math.pow(10, 9)) { // every 10 seconds
                double throughput = count / (delta * Math.pow(10, -9));
                this.throughput.add(throughput);
                count = 0;
                start = end;
            }
        }
    }

    private static class TrackStart<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
        private transient StringClient requestTrackerClient;
        private transient JobControlClient jobControlClient;
        private String trackingServerNameForDiscovery;

        private transient TransientPeriod transientPeriod;

        private TrackStart(String trackingServerNameForDiscovery) {
            this.trackingServerNameForDiscovery = trackingServerNameForDiscovery;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ParameterTool parameterTool = (ParameterTool)
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            jobControlClient = JobControlClient.get(parameterTool);

            Address address = jobControlClient.discoverServer(trackingServerNameForDiscovery);
            requestTrackerClient = new StringClient(address.ip, address.port);
            requestTrackerClient.init();

            transientPeriod = new TransientPeriod();
            transientPeriod.start(parameterTool);
        }

        @Override
        public void close() throws Exception {
            super.close();
            jobControlClient.close();
            requestTrackerClient.close();
        }

        @Override
        public void invoke(T element) throws Exception {
            if (transientPeriod.hasFinished()) {
                requestTrackerClient.send(element.getUniqueRepresentation());
            }
        }
    }

    private static class LatencyTracker<T extends UniquelyRepresentableForTracking> extends RichSinkFunction<T> {
        private transient WithServer requestTracker;
        private transient JobControlClient jobControlClient;
        private final String trackingServerName;
        private TimeDelta currentLatency;
        private boolean firstStart = false;

        private LatencyTracker(String trackingServerName) {
            this.trackingServerName = trackingServerName;
            this.currentLatency = new TimeDelta();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ParameterTool parameterTool = (ParameterTool)
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            jobControlClient = JobControlClient.get(parameterTool);

            requestTracker = new WithServer(new ProcessRequestServer() {
                @Override
                protected void parseRequest(String recordID) {
                    start(recordID);
                }
            });
            requestTracker.open();
            jobControlClient.registerServer(trackingServerName, requestTracker.getMyAddress());

            getRuntimeContext().addAccumulator("tgraph-latency", currentLatency.getNewAccumulator());
        }

        @Override
        public void close() throws Exception {
            super.close();
            requestTracker.close();
            jobControlClient.close();
        }

        @Override
        public synchronized void invoke(T t) throws Exception {
            if (firstStart) {
                currentLatency.end(t.getUniqueRepresentation());
            }
        }

        private synchronized void start(String recordID) {
            currentLatency.start(recordID);
            firstStart = true;
        }
    }

    private static class TransientPeriod {
        private boolean end = false;

        private void start(ParameterTool parameterTool) {
            int secondsTransient = parameterTool.getInt("transient", 30);
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    end = true;
                }
            }, secondsTransient * 1000);
        }

        public boolean hasFinished() {
            return end;
        }
    }
}
