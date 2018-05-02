package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * Created by affo on 02/05/18.
 */
public class EvalUtils {
    public static final double startAmount = 100d;
    public static int sourcePar;
    public static final String sourceSharingGroup = "sources";

    public static int getStartRateFromProps(EvalConfig config) throws IOException {
        // get startInputRate from props file -> this enables us to run the experiments faster
        int startInputRate = config.startInputRate;
        if (startInputRate < 0) {
            if (config.propertiesFile != null) {
                ParameterTool fromProps = ParameterTool.fromPropertiesFile(config.propertiesFile);

                // try to use only the label first
                startInputRate = fromProps.getInt(config.label, -1);

                // use the composite key otherwise
                if (startInputRate < 0) {
                    String strStrategy = config.strategy == Strategy.OPTIMISTIC ? "TB" : "LB"; // timestamp-based or lock-based
                    String key = String.format("%s.%s.%s", config.label, strStrategy, config.isolationLevel.toString());
                    startInputRate = fromProps.getInt(key, -1);
                }
            }

            if (startInputRate < 0) {
                startInputRate = 1000; // set to default
            }
        }

        return startInputRate;
    }

    public static StreamExecutionEnvironment getFlinkEnv(EvalConfig config) {
        sourcePar = config.sourcePar;

        StreamExecutionEnvironment env;
        if (config.isLocal) { // TODO the only way to make slot sharing group work locally for now...
            Configuration conf = new Configuration();
            conf.setInteger("taskmanager.numberOfTaskSlots", config.parallelism + sourcePar);
            env = StreamExecutionEnvironment.createLocalEnvironment(config.parallelism, conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(config.parallelism);
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Flink suggests to keep it within 5 and 10 ms:
        // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html#controlling-latency
        // Anyway, we keep it to 0 to be as reactive as possible when new records are produced.
        // Buffering could lead to unexpected behavior (in terms of performance) in the transaction management.
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        env.getConfig().setGlobalJobParameters(config.params);
        return env;
    }

    public static <T> SingleOutputStreamOperator<T> addToSourcesSharingGroup(
            SingleOutputStreamOperator<T> ds, String operatorName) {
        return ds
                .name(operatorName).setParallelism(sourcePar)
                .slotSharingGroup(sourceSharingGroup);
    }

    public static void setSourcesSharingGroup(TransactionEnvironment tEnv) {
        tEnv.setSourcesSharingGroup(sourceSharingGroup, sourcePar);
    }
}
