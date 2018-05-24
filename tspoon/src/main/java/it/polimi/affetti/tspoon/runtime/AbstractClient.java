package it.polimi.affetti.tspoon.runtime;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by affo on 17/11/16.
 * <p>
 * Service to communicate (a)synchronously with other services.
 */
public abstract class AbstractClient {
    protected final Logger LOG = Logger.getLogger(getClass().getSimpleName());
    public final String address;
    public final int port;

    protected Socket s;
    protected InputStream in;
    protected OutputStream out;

    /**
     * Connects to a server
     * @param address
     * @param port
     */
    public AbstractClient(String address, int port) {
        this.address = address;
        this.port = port;
    }

    /**
     * Reuses an already generated connection
     * @param socket
     * @param in
     * @param out
     */
    public AbstractClient(Socket socket, InputStream in, OutputStream out) {
        this.s = socket;
        this.in = in;
        this.out = out;
        this.address = socket.getInetAddress().getHostAddress();
        this.port = socket.getPort();
    }

    /**
     * If the client has been created to reuse a previous connection,
     * this will throw an IllegalStateException
     *
     * @throws IOException
     * @throws IllegalStateException if inited more than once or if built
     * to reuse a connection
     */
    public void init() throws IOException, IllegalStateException {
        if (s != null || in != null || out != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        s = new Socket(address, port);
        in = s.getInputStream();
        out = s.getOutputStream();
        LOG.info("Connected to " + address + ":" + port);
    }

    public void close() throws IOException {
        LOG.debug("CLOSING client");
        s.close();
        in.close();
        out.close();
    }

    public boolean isClosed() {
        return s.isClosed();
    }
}
