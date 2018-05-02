package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.ObjectClient;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by affo on 13/04/18.
 */
public class WALClient extends ObjectClient implements WAL {
    public WALClient(String addr, int port) {
        super(addr, port);
    }

    @Override
    public void open() throws IOException {
        // does nothing...
        // implementation from WAL
    }

    @Override
    public void addEntry(WAL.Entry entry) throws IOException {
        send(entry);
        receive(); // ACK, wait until entry is persisted
    }

    @Override
    public Iterator<WAL.Entry> replay(String namespace) throws IOException {
        send(namespace);
        return new WALIterator();
    }

    @Override
    public Iterator<Entry> replay(int sourceID, int numberOfSources) throws IOException {
        send(String.format(WALServer.replaySourceFormat, sourceID, numberOfSources));
        return new WALIterator();
    }

    @Override
    public void startSnapshot(long newWM) throws IOException {
        send(String.format(WALServer.startSnapshotFormat, newWM));
        receive(); // wait for the ACK
    }

    @Override
    public void commitSnapshot() throws IOException {
        send(WALServer.commitSnapshotPattern);
    }

    @Override
    public long getSnapshotInProgressWatermark() throws IOException {
        send(WALServer.getCurrentSnapshotWMPattern);
        return (long) receive();
    }

    public static WALClient get(ParameterTool parameters) throws IOException, IllegalArgumentException {
        if (parameters != null && parameters.has("WALServerIP")) {
            String ip = parameters.get("WALServerIP");
            int port = parameters.getInt("WALServerPort");
            WALClient walClient = new WALClient(ip, port);
            walClient.init();
            return walClient;
        } else {
            throw new IllegalArgumentException("Cannot get WALClient without address set in configuration");
        }
    }

    private class WALIterator implements Iterator<WAL.Entry> {
        private long lastTimestamp;
        private WAL.Entry next;

        public WALIterator() {
            next = get();
            lastTimestamp = next.timestamp;
        }

        @Override
        public boolean hasNext() {
            return lastTimestamp >= 0;
        }

        private WAL.Entry get() {
            try {
                WAL.Entry entry = (WAL.Entry) receive();
                lastTimestamp = entry.timestamp;
                return entry;
            } catch (Exception ex) {
                throw new RuntimeException("Problem while replaying WAL: " + ex.getMessage());
            }
        }

        @Override
        public WAL.Entry next() {
            WAL.Entry next = this.next;
            this.next = get();
            return next;
        }
    }
}
