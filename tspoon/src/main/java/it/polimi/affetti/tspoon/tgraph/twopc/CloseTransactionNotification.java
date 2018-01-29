package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by affo on 09/11/17.
 * <p>
 * The message that CloseSink sends to Coordinator
 */
public class CloseTransactionNotification implements Serializable {
    public final int tGraphID;
    public final int timestamp;
    public final Vote vote;
    public final int batchSize;
    public final int replayCause;
    // TODO JSON serialized
    public final String updates;

    private CloseTransactionNotification(
            int timestamp, Vote vote, int batchSize, int replayCause, String updates) {
        this(0, timestamp, vote, batchSize, replayCause, updates);
    }

    private CloseTransactionNotification(
            int tGraphID, int timestamp, Vote vote, int batchSize, int replayCause, String updates) {
        this.tGraphID = tGraphID;
        this.timestamp = timestamp;
        this.vote = vote;
        this.batchSize = batchSize;
        this.replayCause = replayCause;
        this.updates = updates;
    }

    public static CloseTransactionNotification deserialize(String request) {
        String[] tokens = request.split(",");
        int tGraphID = Integer.parseInt(tokens[0]);
        int timestamp = Integer.parseInt(tokens[1]);
        Vote vote = Vote.values()[Integer.parseInt(tokens[2])];
        int batchSize = Integer.parseInt(tokens[3]);
        int replayCause = Integer.parseInt(tokens[4]);

        String updates = "";
        if (tokens.length >= 5) {
            updates = String.join(",", Arrays.copyOfRange(tokens, 5, tokens.length));
        }

        return new CloseTransactionNotification(tGraphID, timestamp, vote, batchSize, replayCause, updates);
    }

    public String serialize() {
        return CloseTransactionNotification.serialize(tGraphID, timestamp, vote, batchSize, replayCause, updates);
    }

    public static String serialize(
            int tGraphID, int timestamp, Vote vote, int batchSize, int replayCause, String updates) {
        String serialized = tGraphID + "," + timestamp + "," + vote.ordinal() + ","
                + batchSize + "," + replayCause;

        if (!updates.isEmpty()) {
            serialized += "," + updates;
        }

        return serialized;
    }

    @Override
    public String toString() {
        return "CloseTransactionNotification{" +
                "tGraphID=" + tGraphID +
                ", timestamp=" + timestamp +
                ", vote=" + vote +
                ", batchSize=" + batchSize +
                ", replayCause=" + replayCause +
                ", updates='" + updates + '\'' +
                '}';
    }
}
