package it.polimi.affetti.tspoon.runtime;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by affo on 12/07/17.
 */
public abstract class LoopingClientHandler extends ClientHandler {
    private boolean stop;

    public LoopingClientHandler(Socket s) {
        super(s);
    }

    @Override
    protected void lifeCycle() throws Exception {
        while (!stop) {
            step();
        }
    }

    protected abstract void step() throws Exception;

    protected void stop() {
        stop = true;
    }

    @Override
    public void close() throws IOException {
        super.close();
        stop();
    }
}
