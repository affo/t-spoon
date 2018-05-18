package it.polimi.affetti.tspoon.tgraph.durability;

/**
 * Created by affo on 17/05/18.
 */

import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;

/**
 * Class for WALService WALEntry
 */
public class WALEntry implements Serializable {
    public Vote vote;
    public long tid, timestamp;
    public Updates updates;

    public WALEntry(Vote vote, long tid, long timestamp, Updates updates) {
        this.vote = vote;
        this.tid = tid;
        this.timestamp = timestamp;
        this.updates = updates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WALEntry entry = (WALEntry) o;

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
        return "WALEntry{" +
                "vote=" + vote +
                ", timestamp=" + timestamp +
                ", updates=" + updates +
                '}';
    }
}