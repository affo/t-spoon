package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.runtime.ObjectClient;
import it.polimi.affetti.tspoon.tgraph.twopc.TRuntimeContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by affo on 13/04/18.
 *
 * Sends snapshot begin/end commands to the central WALService
 * Sends replay commands to every local WALServer
 */
public class WALClient implements WALService {
    private String[] taskManagers;
    private ObjectClient[] localWALServers;

    public WALClient(String[] taskManagers) {
        this.taskManagers = taskManagers;
        this.localWALServers = new ObjectClient[taskManagers.length];
    }

    public static WALClient get(TRuntimeContext tRuntimeContext) throws IOException {
        return get(tRuntimeContext.getTaskManagers());
    }

    public static WALClient get(String[] taskManagers) throws IOException {
        WALClient walClient = new WALClient(taskManagers);
        walClient.open();
        return walClient;
    }

    @Override
    public void open() throws IOException {
        NetUtils.fillWALClients(taskManagers, localWALServers, ObjectClient::new);
    }

    @Override
    public void close() throws IOException {
        for (ObjectClient cli : localWALServers) {
            cli.close();
        }
    }

    @Override
    public Iterator<WALEntry> replay(String namespace) throws IOException {
        return new WALIterator(namespace);
    }

    @Override
    public Iterator<WALEntry> replay(int sourceID, int numberOfSources) throws IOException {
        return new WALIterator(String.format(ProxyWALServer.replaySourceFormat, sourceID, numberOfSources));
    }

    private class WALIterator implements Iterator<WALEntry> {
        private long lastTimestamp;
        private WALEntry next;
        private String request;
        private int cliIndex = 0;
        private ObjectClient currentCli;

        public WALIterator(String request) {
            this.request = request;
            initCli();
            next = get();
            lastTimestamp = next.timestamp;
        }

        @Override
        public boolean hasNext() {
            if (lastTimestamp < 0) {
                cliIndex++;
                if (cliIndex < localWALServers.length) {
                    initCli();
                    this.next = get();
                    return hasNext();
                }
            }

            return !(lastTimestamp < 0 && cliIndex == localWALServers.length);
        }

        private void initCli() {
            currentCli = localWALServers[cliIndex];
            try {
                currentCli.send(request);
            } catch (Exception ex) {
                throw new RuntimeException("Problem while initing ObjectClient: " + ex.getMessage());
            }
        }

        private WALEntry get() {
            try {
                WALEntry entry = (WALEntry) currentCli.receive();
                lastTimestamp = entry.timestamp;
                return entry;
            } catch (Exception ex) {
                throw new RuntimeException("Problem while replaying WALService: " + ex.getMessage());
            }
        }

        @Override
        public WALEntry next() {
            WALEntry next = this.next;
            this.next = get();
            return next;
        }
    }
}
