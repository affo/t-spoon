package it.polimi.affetti.tspoon.runtime;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 04/01/17.
 */
public abstract class AbstractServer implements Runnable {
    protected final String CLASS_NAME = AbstractServer.this.getClass().getSimpleName();
    protected final Logger LOG = Logger.getLogger(CLASS_NAME);

    private int listenPort;
    private ServerSocket srv;
    private final ExecutorService pool;
    private List<ClientHandler> rcvrs;
    private volatile boolean stop;

    public AbstractServer() {
        this.pool = Executors.newCachedThreadPool();
        this.rcvrs = new ArrayList<>();
        this.stop = false;
    }

    protected abstract ClientHandler getHandlerFor(Socket s);

    public void init(int startPort, int endPort) throws IOException {
        if (srv != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        for (; startPort <= endPort; startPort++) {
            try {
                srv = new ServerSocket(startPort);
                listenPort = startPort;
                LOG.info(CLASS_NAME + " listening on " + startPort);
                return;
            } catch (IOException ex) {
                // do nothing, try next port...
                // add random sleep to avoid clash in requests for port
                try {
                    TimeUnit.MILLISECONDS.sleep(new Random().nextLong() % 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // if the program gets here, no port in the range was found
        throw new IOException("no free port found");
    }

    public void init(int listenPort) throws IOException {
        if (srv != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        srv = new ServerSocket(listenPort);
        this.listenPort = listenPort;
        LOG.info(CLASS_NAME + " listening on " + listenPort);
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                Socket s = srv.accept();
                LOG.info(String.format("Connection accepted: %s:%d", s.getLocalAddress(), s.getLocalPort()));
                ClientHandler r = getHandlerFor(s);
                rcvrs.add(r);
                r.init();
                pool.execute(r);
            }
        } catch (SocketException se) {
            // should happen when the socket is closed
            LOG.info(se.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        this.stop = true;
        srv.close();
        for (ClientHandler r : rcvrs) {
            r.close();
        }
        pool.shutdownNow();
    }

    public int getPort() {
        return listenPort;
    }

    public String getIP() throws UnknownHostException {
        return NetUtils.getMyIp();
    }
}