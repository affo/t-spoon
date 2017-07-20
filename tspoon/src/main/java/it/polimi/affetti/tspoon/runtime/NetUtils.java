package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by affo on 25/01/17.
 */
public class NetUtils {
    public static final int QUERY_SERVICE_EXTERNAL_PORT = 8000;
    public static final int QUERY_SERVICE_INTERNAL_PORT = 8001;
    public static final int MIN_PORT = 8010;
    public static final int MAX_PORT = 9000;

    public static String getMyIp() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    public static <T extends AbstractServer> T getServer(int port, T server) throws IOException {
        server.init(port);
        new Thread(server).start();
        return server;
    }

    public static <T extends AbstractServer> T getServer(int startPort, int endPort, T server) throws IOException {
        server.init(startPort, endPort);
        new Thread(server).start();
        return server;
    }
}
