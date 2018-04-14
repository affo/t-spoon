package it.polimi.affetti.tspoon.runtime;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 04/01/17.
 */
public abstract class AbstractServer implements Runnable {
    protected final String CLASS_NAME = AbstractServer.this.getClass().getSimpleName();
    protected final Logger LOG = Logger.getLogger(CLASS_NAME);

    private int listenPort;
    private ServerSocket srv;
    private List<ClientHandler> handlers;
    private List<Thread> executors;
    private volatile boolean stop;

    public AbstractServer() {
        this.handlers = new LinkedList<>();
        this.executors = new LinkedList<>();
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
                open();
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

    /**
     * Hook on init. Override for custom behaviour.
     */
    protected void open() throws IOException {
    }

    public void init(int listenPort) throws IOException {
        if (srv != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        srv = new ServerSocket(listenPort);
        this.listenPort = listenPort;
        open();
        LOG.info(CLASS_NAME + " listening on " + listenPort);
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                Socket s = srv.accept();
                LOG.info(String.format("Connection accepted: %s:%d", s.getLocalAddress(), s.getLocalPort()));
                ClientHandler r = getHandlerFor(s);
                handlers.add(r);
                Thread executor = new Thread(r);
                executors.add(executor);
                r.init();

                executor.start();
            }
        } catch (SocketException se) {
            // should happen when the socket is closed
            LOG.info(se.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws Exception {
        this.stop = true;
        srv.close();
        for (ClientHandler r : handlers) {
            r.close();
        }

        /*
        // waiting for termination could cause deadlock...
        for (Thread t : executors) {
            t.join();
        }
        */
    }

    public int getPort() {
        return listenPort;
    }

    public String getIP() throws UnknownHostException {
        return NetUtils.getMyIp();
    }
}