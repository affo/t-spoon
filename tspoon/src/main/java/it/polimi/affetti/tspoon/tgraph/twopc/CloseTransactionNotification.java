package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by affo on 09/11/17.
 * <p>
 * The message that CloseFunction sends to Coordinator
 */
public class CloseTransactionNotification implements Serializable {
    public final int tGraphID;
    public final int timestamp;
    public final Vote vote;
    public final int batchSize;
    public final int replayCause;

    private CloseTransactionNotification(
            int timestamp, Vote vote, int batchSize, int replayCause) {
        this(0, timestamp, vote, batchSize, replayCause);
    }

    private CloseTransactionNotification(
            int tGraphID, int timestamp, Vote vote, int batchSize, int replayCause) {
        this.tGraphID = tGraphID;
        this.timestamp = timestamp;
        this.vote = vote;
        this.batchSize = batchSize;
        this.replayCause = replayCause;
    }

    public static CloseTransactionNotification deserialize(String request) {
        String[] tokens = request.split(",");
        int tGraphID = Integer.parseInt(tokens[0]);
        int timestamp = Integer.parseInt(tokens[1]);
        Vote vote = Vote.values()[Integer.parseInt(tokens[2])];
        int batchSize = Integer.parseInt(tokens[3]);
        int replayCause = Integer.parseInt(tokens[4]);

        return new CloseTransactionNotification(tGraphID, timestamp, vote, batchSize, replayCause);
    }

    public String serialize() {
        return CloseTransactionNotification.serialize(tGraphID, timestamp, vote, batchSize, replayCause);
    }

    public static String serialize(
            int tGraphID, int timestamp, Vote vote, int batchSize, int replayCause) {

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
