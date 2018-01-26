package it.polimi.affetti.tspoon.tgraph.query;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by affo on 02/08/17.
 * <p>
 * For Optimistic case.
 * <p>
 * There is a single query server per machine.
 * However, requests are sent to a specific query server because it registered the namespace at
 * the JobControlServer, so the QueryServer must notify every subscriber upon query.
 */
public class QueryServer extends AbstractServer {
    private List<QueryListener> listeners = new LinkedList<>();
    private Set<String> registeredNameSpaces = new HashSet<>();
    private RuntimeContext runtimeContext;
    private JobControlClient jobControlClient;

    public QueryServer(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        this.listeners = new LinkedList<>();
    }

    @Override
    public void init(int listenPort) throws IOException {
        super.init(listenPort);
        initJobControlClient();
    }

    @Override
    public void init(int startPort, int endPort) throws IOException {
        super.init(startPort, endPort);
        initJobControlClient();
    }

    @Override
    public void close() throws Exception {
        super.close();
        jobControlClient.close();
    }

    private void initJobControlClient() throws IOException {
        ParameterTool parameterTool = (ParameterTool)
                runtimeContext.getExecutionConfig().getGlobalJobParameters();
        jobControlClient = JobControlClient.get(parameterTool);
    }

    /**
     * For concurrent subscription from different operators
     *
     * @param listener
     */
    public synchronized void listen(QueryListener listener) throws UnknownHostException {
        listeners.add(listener);
        Iterable<String> nameSpaces = listener.getNameSpaces();
        Address address = Address.of(getIP(), getPort());
        for (String nameSpace : nameSpaces) {
            if (!registeredNameSpaces.contains(nameSpace)) {
                jobControlClient.registerServer(nameSpace, address);
                registeredNameSpaces.add(nameSpace);
            }
        }
    }

    @Override
    protected ClientHandler getHandlerFor(Socket s) {
        return new LoopingClientHandler(
                new ObjectClientHandler(s) {
                    @Override
                    protected void lifeCycle() throws Exception {
                        Query query = (Query) receive();
                        for (QueryListener listener : listeners) {
                            listener.onQuery(query); // side-effect on query's result
                        }
                        send(query.getResult());
                    }
                });
    }
}
