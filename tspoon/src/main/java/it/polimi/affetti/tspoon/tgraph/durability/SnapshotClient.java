package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.ObjectClient;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * Created by affo on 13/04/18.
 *
 * Sends snapshot begin/end commands to the central WALService
 * Sends replay commands to every local WALServer
 */
public class SnapshotClient extends ObjectClient implements SnapshotService {
    public SnapshotClient(String addr, int port) {
        super(addr, port);
    }

    @Override
    public void open() throws IOException {
        init();
    }

    @Override
    public void startSnapshot(long newWM) throws IOException {
        send(String.format(ProxyWALServer.startSnapshotFormat, newWM));
        receive(); // wait for the ACK
    }

    @Override
    public long getSnapshotInProgressWatermark() throws IOException {
        send(ProxyWALServer.getCurrentSnapshotWMPattern);
        return (long) receive();
    }

    public static SnapshotClient get(ParameterTool parameters) throws IOException, IllegalArgumentException {
        if (parameters != null && parameters.has("WALServerIP")) {
            String ip = parameters.get("WALServerIP");
            int port = parameters.getInt("WALServerPort");
            SnapshotClient walClient = new SnapshotClient(ip, port);
            walClient.init();
            return walClient;
        } else {
            throw new IllegalArgumentException("Cannot get WALClient without address set in configuration");
        }
    }
}
