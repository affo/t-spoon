package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * Created by affo on 02/05/18.
 */
public class EvalConfig {
    public ParameterTool params;
    public String label;
    public int sourcePar;
    public IsolationLevel isolationLevel;
    public int parallelism;
    public int partitioning;
    public int noTGraphs;
    public int noStates;
    public int keySpaceSize;
    public boolean seriesOrParallel;
    public boolean useDependencyTracking;
    public boolean synchronous;
    public boolean durable;
    public int openServerPoolSize, stateServerPoolSize, queryServerPoolSize;
    public boolean printPlan;
    public boolean baselineMode;
    public int batchSize;
    public int resolution;
    public int maxNumberOfBatches;
    public Strategy strategy;
    public int startInputRate;
    public String propertiesFile;
    public boolean isLocal;

    public static EvalConfig fromParams(ParameterTool parameters) throws IOException {
        EvalConfig config = new EvalConfig();

        config.label = parameters.get("label");
        config.isLocal = config.label == null || config.label.startsWith("local");
        config.propertiesFile = parameters.get("propsFile", null);
        config.sourcePar = parameters.getInt("sourcePar", 1);

        int isolationLevelNumber = parameters.getInt("isolationLevel", 3);
        config.isolationLevel = IsolationLevel.values()[isolationLevelNumber];
        if (config.isolationLevel == IsolationLevel.PL4) {
            config.sourcePar = 1;
        }

        config.parallelism = parameters.getInt("par", 4) - config.sourcePar;
        config.partitioning = parameters.getInt("partitioning", 4) - config.sourcePar;
        config.noTGraphs = parameters.getInt("noTG", 1);
        config.noStates = parameters.getInt("noStates", 1);
        config.keySpaceSize = parameters.getInt("ks", 100000);
        config.seriesOrParallel = parameters.getBoolean("series", true);
        boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        config.strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        config.useDependencyTracking = parameters.getBoolean("dependencyTracking", true);
        config.synchronous = parameters.getBoolean("synchronous", false);
        config.durable = parameters.getBoolean("durable", false);
        config.openServerPoolSize = parameters.getInt("openPool", 1);
        config.stateServerPoolSize = parameters.getInt("statePool", 1);
        config.queryServerPoolSize = parameters.getInt("queryPool", 1);
        config.batchSize = parameters.getInt("batchSize", 100000);
        config.resolution = parameters.getInt("resolution", 100);
        config.maxNumberOfBatches = parameters.getInt("numberOfBatches", -1);
        config.startInputRate = parameters.getInt("startInputRate", -1);

        config.startInputRate = EvalUtils.getStartRateFromProps(config);

        // debugging stuff
        config.printPlan = parameters.getBoolean("printPlan", false);
        config.baselineMode = parameters.getBoolean("baseline", false);

        // merge the value obtained for startInputRate for later checking
        parameters.toMap().put("startInputRate", String.valueOf(config.startInputRate));
        config.params = parameters;

        return config;
    }
}
