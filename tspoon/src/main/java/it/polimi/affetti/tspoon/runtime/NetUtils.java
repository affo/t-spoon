package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.evaluation.EvalConfig;
import it.polimi.affetti.tspoon.tgraph.durability.ProxyWALServer;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Created by affo on 25/01/17.
 */
public class NetUtils {
    public static final int JOB_CONTROL_PORT = 1234;
    public static final int GLOBAL_WAL_SERVER_PORT = 1235;
    public static final int LOCAL_WAL_SERVER_PORT = 1236;
    public static final int MIN_PORT = 9000;
    public static final int MAX_PORT = 50000;

    // ensure that singleton servers' base port is at list at the specified distance
    // in order to have more than 1 server in the pool, or to make it possible
    // to have more than 1 TM per machine.
    public static final int SERVER_POOL_PORT_DISTANCE = 100;

    public static final int OPEN_SERVER_PORT = 8000;
    public static final int STATE_SERVER_PORT = OPEN_SERVER_PORT + SERVER_POOL_PORT_DISTANCE;
    public static final int QUERY_SERVER_PORT = STATE_SERVER_PORT + SERVER_POOL_PORT_DISTANCE;

    public enum ServerType {
        OPEN(OPEN_SERVER_PORT),
        STATE(STATE_SERVER_PORT),
        QUERY(QUERY_SERVER_PORT),
        WAL(LOCAL_WAL_SERVER_PORT);

        private final int basePort;

        ServerType(int basePort) {
            this.basePort = basePort;
        }

        public int getBasePort() {
            return basePort;
        }

        public int getLastPort() {
            return getBasePort() + SERVER_POOL_PORT_DISTANCE - 1;
        }
    }

    private static Map<ServerType, AbstractServer[]> serverPools = new HashMap<>();

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

    public static <T extends AbstractServer> T getServer(ServerType serverType, T server) throws IOException {
        return getServer(serverType.getBasePort(), serverType.getLastPort(), server);
    }

    public static JobControlServer launchJobControlServer(ParameterTool parameters) throws IOException {
        parameters.toMap().put("jobControlServerIP", getMyIp());
        parameters.toMap().put("jobControlServerPort", String.valueOf(JOB_CONTROL_PORT));
        return getServer(JOB_CONTROL_PORT, new JobControlServer());
    }

    public static ProxyWALServer launchWALServer(ParameterTool parameters, EvalConfig config) throws IOException {
        return launchWALServer(parameters, config.sourcePar, config.taskManagerIPs);
    }

    public static ProxyWALServer launchWALServer(
            ParameterTool parameters, int numberOfOpens, String[] taskManagerIPs) throws IOException {
        parameters.toMap().put("WALServerIP", getMyIp());
        parameters.toMap().put("WALServerPort", String.valueOf(GLOBAL_WAL_SERVER_PORT));
        return getServer(GLOBAL_WAL_SERVER_PORT,
                new ProxyWALServer(numberOfOpens, taskManagerIPs));
    }

    public static <T extends ObjectClient> void fillWALClients(
            String[] taskManagers, T[] clients, BiFunction<String, Integer, T> clientSupplier) throws IOException {
        Map<String, Integer> portPerIP = new HashMap<>();
        for (int i = 0; i < clients.length; i++) {
            String tmAddress = taskManagers[i];
            // in case of ubiquitous localServers
            Integer port = portPerIP.get(tmAddress);
            if (port == null) {
                port = LOCAL_WAL_SERVER_PORT;
            } else {
                // we already got it
                port++;
            }
            portPerIP.put(tmAddress, port);

            clients[i] = clientSupplier.apply(taskManagers[i], port);

            int tries = 1000;
            boolean trying = true;

            while (trying) {
                try {
                    clients[i].init();
                    trying = false;
                } catch (ConnectException ce) {
                    if (tries == 0) {
                        throw ce;
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(
                                "Interrupted while trying to connect to " + taskManagers[i] + ":" + port);
                    }
                    tries--;
                }
            }
        }
    }

    public synchronized static <T extends AbstractServer> T openAsSingleton(
            ServerType serverType, Supplier<T> supplier) throws IOException {
        return openInPool(serverType, supplier, 1, 0);
    }

    public synchronized static <T extends AbstractServer> T openInPool(
            ServerType serverType, Supplier<T> supplier, int taskNumber) throws IOException {
        return openInPool(serverType, supplier, Runtime.getRuntime().availableProcessors(), taskNumber);
    }

    @SuppressWarnings("unchecked")
    public synchronized static <T extends AbstractServer> T openInPool(
            ServerType serverType, Supplier<T> supplier, int poolSize, int taskNumber) throws IOException {
        int index = taskNumber % poolSize;
        AbstractServer[] servers = serverPools.computeIfAbsent(serverType, st -> new AbstractServer[poolSize]);
        AbstractServer server = servers[index];
        if (server == null) {
            server = supplier.get();
            int basePort = serverType.getBasePort();
            getServer(basePort, basePort + SERVER_POOL_PORT_DISTANCE - 1, server);
            servers[index] = server;
        }

        return (T) server;
    }

    public synchronized static void closeServerPool(ServerType serverType) throws Exception {
        // if we remove the server we allow for multiple open/close in the same JVM process.
        // However we introduce the possibility for singleton violation (concurrent open/close).
        // Open/close cycles should be clearly isolated by a delta time or equivalent...
        AbstractServer[] servers = serverPools.remove(serverType);
        if (servers != null) {
            for (AbstractServer server : servers) {
                server.close();
            }
        }
    }
}
