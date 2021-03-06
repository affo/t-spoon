package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.common.Address;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by affo on 26/07/17.
 */
public class JobControlClient extends StringClient {
    private boolean tunable;

    public JobControlClient(String address, int port) {
        super(address, port);
    }

    public static JobControlClient get(ParameterTool parameters) throws IOException, IllegalArgumentException {
        if (parameters != null && parameters.has("jobControlServerIP")) {
            String ip = parameters.get("jobControlServerIP");
            int port = parameters.getInt("jobControlServerPort");
            boolean tunable = parameters.getBoolean("tunable", false);
            JobControlClient jobControlClient = new JobControlClient(ip, port);
            jobControlClient.setTunable(tunable);
            jobControlClient.init();
            return jobControlClient;
        } else {
            throw new IllegalArgumentException("Cannot get JobControlClient without address set in configuration");
        }
    }

    public void setTunable(boolean tunable) {
        this.tunable = tunable;
    }

    public boolean isTunable() {
        return tunable;
    }

    public void observe(JobControlListener listener) throws IOException {
        JobControlObserver.open(address, port).observe(listener);
    }

    public void publish(String message) {
        // no problem in concurrent send and receive
        this.send(message);
    }

    public void publishFinishMessage() {
        this.publish(JobControlObserver.finishPattern);
    }

    public void terminateJobExceptionally(String exceptionMessage) {
        this.publish(String.format(JobControlObserver.finishedExceptionallyFormat, exceptionMessage));
    }

    public void publishBatchEnd() {
        this.publish(JobControlObserver.batchEndPattern);
    }

    public void registerServer(String label, Address address) {
        this.send(String.format(JobControlServer.registerFormat, label, address.ip, address.port));
    }

    public Set<Address> discoverServers(String label) throws IOException {
        this.send(String.format(JobControlServer.discoverFormat, label));
        String response = receive();

        if (response == null) {
            throw new IOException("Null response received: " + response);
        }

        Set<Address> addresses = new HashSet<>();

        for (String ipPort : response.split(",")) {
            String[] tokens = ipPort.split(":");
            addresses.add(Address.of(tokens[0], Integer.parseInt(tokens[1])));
        }

        return addresses;
    }

    /**
     * Returns only the first registered server for the provided label
     * @param label
     * @return
     * @throws IOException
     */
    public Address discoverServer(String label) throws IOException {
        this.send(String.format(JobControlServer.discoverFormat, label));
        String response = receive();

        if (response == null) {
            throw new IOException("Null response received: " + response);
        }

        String[] ips = response.split(",");
        String[] tokens = ips[0].split(":");
        return Address.of(tokens[0], Integer.parseInt(tokens[1]));
    }
}
