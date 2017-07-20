package it.polimi.affetti.tspoon.tgraph;

/**
 * Created by affo on 16/03/17.
 */
public enum Vote {
    COMMIT, ABORT, REPLAY;

    public Vote merge(Vote other) {
        if (this == REPLAY) {
            return REPLAY;
        }

        switch (other) {
            case COMMIT:
                return this;
            default:
                return other;
        }
    }
}
