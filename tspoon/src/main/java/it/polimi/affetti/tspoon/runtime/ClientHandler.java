package it.polimi.affetti.tspoon.runtime;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by affo on 12/07/17.
 */
public abstract class ClientHandler implements Runnable {
    protected final Logger LOG = Logger.getLogger(ClientHandler.this.getClass().getSimpleName());

    protected Socket socket;
    protected BufferedReader in;
    protected PrintWriter out;

    public ClientHandler(Socket s) {
        this.socket = s;
    }

    protected void initStreams() throws IOException {
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
    }

    protected void init() throws IOException {
        initStreams();
    }

    protected abstract void lifeCycle() throws Exception;

    protected void send(String msg) {
        out.println(msg);
    }

    protected String receive() throws IOException {
        return in.readLine();
    }

    @Override
    public void run() {
        try {
            lifeCycle();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            //e.printStackTrace();
        }
    }

    public void close() throws IOException {
        socket.close();
        in.close();
        out.close();
    }
}
