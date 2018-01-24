package it.polimi.affetti.tspoon.tgraph;

/**
 * Created by affo on 17/07/17.
 */
public enum IsolationLevel {
    PL0, PL1, PL2, PL3, PL4;

    public boolean gte(IsolationLevel other) {
        return this.ordinal() >= other.ordinal();
    }
}
