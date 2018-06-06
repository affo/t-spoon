package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.backed.Transfer;
import it.polimi.affetti.tspoon.tgraph.backed.TransferID;
import it.polimi.affetti.tspoon.tgraph.backed.TunableTransferSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This class is for testing the backpressure mechanism
 */
public class PureRates {
    public static final String RECORD_TRACKING_SERVER_NAME = "request-tracker";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        EvalConfig config = EvalConfig.fromParams(parameters);
        int heaviness = parameters.getInt("heaviness", 10);

        NetUtils.launchJobControlServer(parameters);
        StreamExecutionEnvironment env = config.getFlinkEnv();

        TunableTransferSource tunableSource =
                new TunableTransferSource(config, RECORD_TRACKING_SERVER_NAME);

        DataStreamSource<TransferID> dsSource = env.addSource(tunableSource);
        SingleOutputStreamOperator<TransferID> tidSource =
                config.addToSourcesSharingGroup(dsSource, "TunableSource");
        SingleOutputStreamOperator<Transfer> toTranfers = tidSource
                .map(new TunableTransferSource.ToTransfers(config.keySpaceSize, EvalConfig.startAmount));
        DataStream<Transfer> transfers = config.addToSourcesSharingGroup(toTranfers, "ToTransfers");

        transfers = transfers.map(new HeavyComputation(heaviness))
                .returns(Transfer.class)
                .slotSharingGroup("default")
                .name("HeavyComputation")
                .disableChaining();

        EndToEndTracker endEndToEndTracker = new EndToEndTracker(false);

        SingleOutputStreamOperator<Transfer> afterEndTracking = transfers
                .process(endEndToEndTracker)
                .setParallelism(1)
                .name("EndTracker")
                .setBufferTimeout(0);

        DataStream<TransferID> endTracking = afterEndTracking
                .getSideOutput(endEndToEndTracker.getRecordTracking());

        endTracking
                .addSink(new Tracker<>(RECORD_TRACKING_SERVER_NAME))
                .setParallelism(1).name("EndTracker");

        env.execute("PureRates");
    }

    private static class HeavyComputation implements MapFunction<Transfer, Transfer> {
        private int heaviness;

        public HeavyComputation(int heaviness) {
            this.heaviness = heaviness;
        }

        @Override
        public Transfer map(Transfer transfer) throws Exception {
            // let's find the first N prime numbers
            int count = 0;
            int num = 1;

            while (count < heaviness) {
                boolean prime = true;
                for (int i = 2; i <= Math.sqrt(num) && prime; i++) {
                    if (num % i == 0) {
                        prime = false;
                    }
                }

                if (prime) {
                    count++;
                }
            }

            return transfer;
        }
    }
}
