package it.polimi.affetti.tspoon.test;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.backed.*;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import org.apache.flink.api.common.JobExecutionResult;

import java.util.List;

/**
 * Created by affo on 26/07/17.
 */
public class TransferTestUtils {
    public static JobExecutionResult populateResults(
            int statePartitioning, Strategy strategy, IsolationLevel isolationLevel,
            int limit, int noAccounts, double startAmount) throws Exception {
        TransferGraph transferGraph = new TransferGraph(statePartitioning, startAmount, strategy, isolationLevel) {
            @Override
            protected TransferSource getTransfers() {
                return new TransferSource(limit, noAccounts, startAmount);
            }
        };
        return doTheRest(transferGraph);
    }

    public static JobExecutionResult populateResults(
            int statePartitioning, Strategy strategy, IsolationLevel isolationLevel,
            double startAmount, Transfer... transfers) throws Exception {
        TransferGraph transferGraph = new TransferGraph(statePartitioning, startAmount, strategy, isolationLevel) {
            @Override
            protected TransferSource getTransfers() {
                return new TransferSource(transfers);
            }
        };
        return doTheRest(transferGraph);
    }

    private static JobExecutionResult doTheRest(TransferGraph transferGraph) throws Exception {
        TGraphOutput<Movement, Double> output = transferGraph.draw();
        ResultUtils.addAccumulator(output.watermarks, "watermarks");
        ResultUtils.addAccumulator(output.updates, "updates");
        ResultUtils.addAccumulator(output.output, "output");

        return transferGraph.execute();
    }

    public static List<Integer> getWatermarks(JobExecutionResult result) {
        return ResultUtils.extractResult(result, "watermarks");
    }

    public static List<Update<Double>> getUpdates(JobExecutionResult result) {
        return ResultUtils.extractResult(result, "updates");
    }

    public static List<TransactionResult<Movement>> getOutput(JobExecutionResult result) {
        return ResultUtils.extractResult(result, "output");
    }
}
