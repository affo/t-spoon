package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.common.Address;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class ClientServerTestDrive {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(0);

        // get input data
        DataStream<Tuple3<String, Integer, Long>> s = env.generateSequence(0, 100).map(
                new RichMapFunction<Long, Tuple3<String, Integer, Long>>() {
                    private transient WithServer server;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        server = new WithServer(new EchoServer());
                        server.open();
                    }

                    @Override
                    public Tuple3<String, Integer, Long> map(Long n) throws Exception {
                        if (n == 100) {
                            Thread.sleep(2000);
                        }

                        Address address = server.getMyAddress();
                        return Tuple3.of(address.ip, address.port, n);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        server.close();
                    }
                }
        );

        s.addSink(new RichSinkFunction<Tuple3<String, Integer, Long>>() {
            private StringClient textClient;

            @Override
            public void invoke(Tuple3<String, Integer, Long> e) throws Exception {
                if (textClient == null) {
                    textClient = new StringClient(e.f0, e.f1);
                    textClient.init();
                }

                textClient.send("From downstream: " + e.f2);
            }

            @Override
            public void close() throws Exception {
                super.close();
                textClient.close();
            }
        });

        env.execute();
    }

    private static class EchoServer extends ProcessRequestServer {

        @Override
        protected void parseRequest(String request) {
            System.out.println(request);
        }
    }
}
