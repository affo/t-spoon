package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by affo on 02/08/17.
 */
public class ObjectClient extends AbstractClient {
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private int noMessagesSent = 0;

    public ObjectClient(String address, int port) {
        super(address, port);
    }

    public ObjectClient(Socket socket, ObjectInputStream in, ObjectOutputStream out) {
        super(socket, in, out);
        this.in = in;
        this.out = out;
    }

    @Override
    public void init() throws IOException {
        super.init();
        this.out = new ObjectOutputStream(super.out);
        out.flush();
        this.in = new ObjectInputStream(super.in);
    }

    public void send(Object data) throws IOException {
        out.writeUnshared(data);
        noMessagesSent++;

        // prevent memory leak
        if (noMessagesSent % 100000 == 0) {
            out.reset();
        }
    }

    public Object receive() throws IOException {
        try {
            return in.readUnshared();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found while deserializing message: " + e.getMessage());
        }
    }
}
