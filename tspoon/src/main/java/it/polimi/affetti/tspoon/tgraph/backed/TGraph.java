package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.runtime.JobControlServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * Created by affo on 26/07/17.
 */
public abstract class TGraph<O, S> {
    public TGraphOutput<O, S> output;

    public final Strategy strategy;
    public final IsolationLevel isolationLevel;
    private StreamExecutionEnvironment env;

    public TGraph(Strategy strategy, IsolationLevel isolationLevel) {
        this.strategy = strategy;
        this.isolationLevel = isolationLevel;
    }

    private StreamExecutionEnvironment getEnv() {
        if (env == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setBufferTimeout(0);
        }
        return env;
    }

    public TGraphOutput<O, S> draw() {
        this.output = doDraw(getEnv());
        return output;
    }

    protected abstract TGraphOutput<O, S> doDraw(StreamExecutionEnvironment env);

    public JobExecutionResult execute() throws Exception {
        ParameterTool parameters = ParameterTool.fromMap(new HashMap<>());
        JobControlServer jobControlServer = NetUtils.launchJobControlServer(parameters);
        getEnv().getConfig().setGlobalJobParameters(parameters);
        JobExecutionResult result = getEnv().execute();
        jobControlServer.close();
        return result;
    }
}