package it.polimi.affetti.tspoon.runtime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * Created by affo on 12/07/17.
 */
public class StringClient extends AbstractClient {
    public StringClient(String addr, int port) {
        super(addr, port);
    }

    protected BufferedReader in;
    protected PrintWriter out;

    public void init() throws IOException {
        super.init();
        this.in = new BufferedReader(new InputStreamReader(super.in));
        this.out = new PrintWriter(super.out);
    }

    public void send(String request) {
        out.println(request);
        out.flush();
    }

    public String receive() throws IOException {
        return in.readLine();
    }
}
