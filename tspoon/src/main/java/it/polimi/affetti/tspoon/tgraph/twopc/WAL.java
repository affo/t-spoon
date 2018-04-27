package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by affo on 31/07/17.
 */
public interface WAL {
    void open() throws IOException;

    void close() throws IOException;

    void addEntry(Entry entry) throws IOException;

    /**
     * Namespace can be '*' for every entry
     * @param namespace
     * @return
     * @throws IOException
     */
    Iterator<Entry> replay(String namespace) throws IOException;

    // ----------------- Snapshotting

    void startSnapshot(long newWM) throws IOException;

    void commitSnapshot() throws IOException;

    long getSnapshotInProgressWatermark() throws IOException;

    /**
     * Class for WAL Entry
     */
    class Entry implements Serializable {
        public Vote vote;
        public long tid, timestamp;
        public Updates updates;

        public Entry(Vote vote, long tid, long timestamp, Updates updates) {
            this.vote = vote;
            this.tid = tid;
            this.timestamp = timestamp;
            this.updates = updates;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (timestamp != entry.timestamp) return false;
            if (vote != entry.vote) return false;
            return updates != null ? updates.equals(entry.updates) : entry.updates == null;
        }

        @Override
        public int hashCode() {
            int result = vote != null ? vote.hashCode() : 0;
            result = 31 * result + (int) (tid ^ (tid >>> 32));
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (updates != null ? updates.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "vote=" + vote +
                    ", timestamp=" + timestamp +
                    ", updates=" + updates +
                    '}';
        }
    }
}
