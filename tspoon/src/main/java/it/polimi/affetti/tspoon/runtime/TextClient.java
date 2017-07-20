package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;

/**
 * Created by affo on 12/07/17.
 */
public class TextClient extends AbstractClient {
    public TextClient(String addr, int port) {
        super(addr, port);
    }

    public void text(String msg) throws IOException {
        send(msg);
    }
}
