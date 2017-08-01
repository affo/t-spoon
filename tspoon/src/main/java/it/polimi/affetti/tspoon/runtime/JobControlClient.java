package it.polimi.affetti.tspoon.runtime;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * Created by affo on 26/07/17.
 */
public class JobControlClient extends AbstractClient implements Runnable {
    public static final String finishPattern = "FINISHED";

    private boolean stop;
    private JobControlListener listener;

    private JobControlClient(String addr, int port) {
        super(addr, port);
    }

    public static JobControlClient get(ParameterTool parameters) throws IOException {
        JobControlClient jobControlClient = null;
        if (parameters.has("jobControlServerIP")) {
            String ip = parameters.get("jobControlServerIP");
            int port = parameters.getInt("jobControlServerPort");
            jobControlClient = new JobControlClient(ip, port);
            jobControlClient.init();
        }

        return jobControlClient;
    }

    public void stop() {
        stop = true;
    }

    public void observe(JobControlListener listener) {
        this.listener = listener;
        // TODO maybe we will need a thread pool
        new Thread(this).start();
        send(JobControlServer.subscribePattern);
    }

    public void publish(String message) {
        // no problem in concurrent send and receive
        this.send(message);
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                String message = recv();
                if (message == null) {
                    break;
                }

                LOG.info("Received notification " + message);
                processNotification(message);
            }
        } catch (IOException e) {
            LOG.error("Exception while observing: " + e.getMessage());
        }
    }

    private void processNotification(String message) {
        switch (message) {
            case finishPattern:
                listener.onJobFinish();
                break;
        }
    }
}
