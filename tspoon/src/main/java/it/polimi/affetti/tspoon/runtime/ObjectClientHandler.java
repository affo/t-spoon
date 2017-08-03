package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by affo on 02/08/17.
 */
public abstract class ObjectClientHandler extends ClientHandler {
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private int noMessagesSent = 0;

    public ObjectClientHandler(Socket s) {
        super(s);
    }

    @Override
    protected void init() throws IOException {
        super.init();
        this.out = new ObjectOutputStream(super.out);
        out.flush();
        this.in = new ObjectInputStream(super.in);
    }

    protected void send(Object data) throws IOException {
        out.writeUnshared(data);
        noMessagesSent++;

        // prevent memory leak
        if (noMessagesSent % 100000 == 0) {
            out.reset();
        }
    }

    protected Object receive() throws IOException, ClassNotFoundException {
        return in.readUnshared();
    }
}
