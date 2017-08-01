package it.polimi.affetti.tspoon.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * Created by affo on 28/07/17.
 * <p>
 * Launch a streaming job and use the methods provided to accumulate results.
 * <p>
 * You can use the baked classes in tgraph.baked to generate streaming jobs easily.
 */
public class ResultUtils {
    /**
     * @param out a DataStream. Can be extracted from a GraphOutput
     * @param accName
     * @param <T>
     */
    public static <T> void addAccumulator(DataStream<T> out, String accName) {
        out.addSink(new AccumulatingSink<>(accName)).setParallelism(1);
    }

    /**
     * For use by extending classes
     *
     * @param result
     * @param accName
     * @param <T>
     * @return
     */
    public static <T> List<T> extractResult(JobExecutionResult result, String accName) {
        return result.getAccumulatorResult(accName);
    }
}
