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
    public final int timestamp;
    public final Vote vote;
    public final int batchSize;
    public final int replayCause;
    // TODO JSON serialized
    public final String updates;

    private CloseTransactionNotification(
            int timestamp, Vote vote, int batchSize, int replayCause, String updates) {
        this.timestamp = timestamp;
        this.vote = vote;
        this.batchSize = batchSize;
        this.replayCause = replayCause;
        this.updates = updates;
    }

    public static CloseTransactionNotification deserialize(String request) {
        String[] tokens = request.split(",");
        int timestamp = Integer.parseInt(tokens[0]);
        Vote vote = Vote.values()[Integer.parseInt(tokens[1])];
        int batchSize = Integer.parseInt(tokens[2]);
        int replayCause = Integer.parseInt(tokens[3]);

        String updates = "";
        if (tokens.length >= 4) {
            updates = String.join(",", Arrays.copyOfRange(tokens, 4, tokens.length));
        }

        return new CloseTransactionNotification(timestamp, vote, batchSize, replayCause, updates);
    }

    public static String serialize(int timestamp, Vote vote, int batchSize, int replayCause, String updates) {
        String serialized = timestamp + "," + vote.ordinal() + ","
                + batchSize + "," + replayCause;

        if (!updates.isEmpty()) {
            serialized += "," + updates;
        }

        return serialized;
    }
}
