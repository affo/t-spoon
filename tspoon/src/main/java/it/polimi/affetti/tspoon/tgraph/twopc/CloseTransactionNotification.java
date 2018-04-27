package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;

/**
 * Created by affo on 09/11/17.
 * <p>
 * The message that CloseFunction sends to Coordinator
 */
public class CloseTransactionNotification implements Serializable {
    public final int tGraphID;
    public final long timestamp;
    public final Vote vote;
    public final int batchSize;
    public final long replayCause;

    private CloseTransactionNotification(
            long timestamp, Vote vote, int batchSize, long replayCause) {
        this(0, timestamp, vote, batchSize, replayCause);
    }

    private CloseTransactionNotification(
            int tGraphID, long timestamp, Vote vote, int batchSize, long replayCause) {
        this.tGraphID = tGraphID;
        this.timestamp = timestamp;
        this.vote = vote;
        this.batchSize = batchSize;
        this.replayCause = replayCause;
    }

    public static CloseTransactionNotification deserialize(String request) {
        String[] tokens = request.split(",");
        int tGraphID = Integer.parseInt(tokens[0]);
        long timestamp = Long.parseLong(tokens[1]);
        Vote vote = Vote.values()[Integer.parseInt(tokens[2])];
        int batchSize = Integer.parseInt(tokens[3]);
        long replayCause = Long.parseLong(tokens[4]);

        return new CloseTransactionNotification(tGraphID, timestamp, vote, batchSize, replayCause);
    }

    public String serialize() {
        return CloseTransactionNotification.serialize(tGraphID, timestamp, vote, batchSize, replayCause);
    }

    public static String serialize(
            int tGraphID, long timestamp, Vote vote, int batchSize, long replayCause) {

        return tGraphID + "," + timestamp + "," + vote.ordinal() + ","
                + batchSize + "," + replayCause;
    }

    @Override
    public String toString() {
        return "CloseTransactionNotification{" +
                "tGraphID=" + tGraphID +
                ", timestamp=" + timestamp +
                ", vote=" + vote +
                ", batchSize=" + batchSize +
                ", replayCause=" + replayCause +
                '}';
    }
}
