package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.runtime.ObjectClient;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

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

    public SnapshotClient(Socket socket, ObjectInputStream in, ObjectOutputStream out) {
        super(socket, in, out);
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
        Address proxyWALServerAddress = NetUtils.getProxyWALServerAddress(parameters);
        SnapshotClient walClient = new SnapshotClient(proxyWALServerAddress.ip, proxyWALServerAddress.port);
        walClient.init();
        return walClient;
    }
}
