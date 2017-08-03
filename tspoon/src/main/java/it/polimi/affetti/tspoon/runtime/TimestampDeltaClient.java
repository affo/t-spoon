package it.polimi.affetti.tspoon.runtime;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * Created by affo on 01/08/17.
 */
public class TimestampDeltaClient extends StringClient {
    private final String beginFormat = ">%s.%s";
    private final String endFormat = "<%s.%s";

    private TimestampDeltaClient(String addr, int port) {
        super(addr, port);
    }

    public static TimestampDeltaClient get(ParameterTool parameters) throws IOException {
        TimestampDeltaClient deltaClient = null;
        if (parameters.has("deltaServerIP")) {
            String ip = parameters.get("deltaServerIP");
            int port = parameters.getInt("deltaServerPort");
            deltaClient = new TimestampDeltaClient(ip, port);
            deltaClient.init();
        }

        return deltaClient;
    }

    public void begin(String metricName, String id) {
        send(String.format(beginFormat, metricName, id));
    }

    public void end(String metricName, String id) {
        send(String.format(endFormat, metricName, id));
    }
}
