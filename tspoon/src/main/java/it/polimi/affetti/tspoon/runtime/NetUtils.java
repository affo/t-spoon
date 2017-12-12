package it.polimi.affetti.tspoon.runtime;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by affo on 25/01/17.
 */
public class NetUtils {
    public static final int JOB_CONTROL_PORT = 3000;
    public static final int MIN_PORT = 9000;
    public static final int MAX_PORT = 10000;

    // ensure that singleton servers' base port is at list at the distance
    // specified in order to make it possible to have more than 1 TM per machine
    public static final int SINGLETON_SERVER_PORT_DISTANCE = 10;

    public static final int OPEN_SERVER_PORT = 8000;
    public static final int STATE_SERVER_PORT = 8010;

    public enum SingletonServerType {
        OPEN(OPEN_SERVER_PORT),
        STATE(STATE_SERVER_PORT);

        private final int port;

        SingletonServerType(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }

    private static Map<SingletonServerType, AbstractServer> singletonServers = new HashMap<>();

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

    public static JobControlServer launchJobControlServer(ParameterTool parameters) throws IOException {
        parameters.toMap().put("jobControlServerIP", getMyIp());
        parameters.toMap().put("jobControlServerPort", String.valueOf(JOB_CONTROL_PORT));
        return getServer(JOB_CONTROL_PORT, new JobControlServer());
    }

    @SuppressWarnings("unchecked")
    public synchronized static <T extends AbstractServer> T openAsSingleton(
            SingletonServerType serverType, Supplier<T> supplier) throws IOException {
        AbstractServer server = singletonServers.get(serverType);
        if (server == null) {
            server = supplier.get();
            int basePort = serverType.getPort();
            getServer(basePort, basePort + SINGLETON_SERVER_PORT_DISTANCE - 1, server);
            singletonServers.put(serverType, server);
        }

        return (T) server;
    }

    public synchronized static void closeAsSingleton(SingletonServerType serverType) throws Exception {
        singletonServers.remove(serverType).close();
    }
}
