package it.polimi.affetti.tspoon.tgraph.backed;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 28/07/17.
 */
public abstract class Graph<O> {
    public GraphOutput<O> output;
    private StreamExecutionEnvironment env;

    private StreamExecutionEnvironment getEnv() {
        if (env == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        return env;
    }

    public GraphOutput<O> draw() {
        this.output = doDraw(getEnv());
        return output;
    }

    protected abstract GraphOutput<O> doDraw(StreamExecutionEnvironment env);

    public JobExecutionResult execute() throws Exception {
        return getEnv().execute();
    }
}
