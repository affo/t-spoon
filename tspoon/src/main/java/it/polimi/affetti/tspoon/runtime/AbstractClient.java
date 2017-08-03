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
    private String addr;
    private int port;

    protected Socket s;
    protected InputStream in;
    protected OutputStream out;

    public AbstractClient(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public void init() throws IOException {
        if (s != null) {
            throw new IllegalStateException("Cannot init more than once");
        }

        s = new Socket(addr, port);
        in = s.getInputStream();
        out = s.getOutputStream();
        LOG.info("Connected to " + addr + ":" + port);
    }

    public void close() throws IOException {
        LOG.debug("CLOSING client");
        s.close();
        in.close();
        out.close();
    }
}
