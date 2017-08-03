package it.polimi.affetti.tspoon.runtime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by affo on 02/08/17.
 */
public abstract class StringClientHandler extends ClientHandler {
    protected BufferedReader in;
    protected PrintWriter out;

    public StringClientHandler(Socket s) {
        super(s);
    }

    @Override
    protected void init() throws IOException {
        super.init();
        this.in = new BufferedReader(new InputStreamReader(super.in));
        this.out = new PrintWriter(super.out, true);
    }

    protected void send(String msg) {
        out.println(msg);
    }

    protected String receive() throws IOException {
        return in.readLine();
    }
}
