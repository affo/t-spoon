package it.polimi.affetti.tspoon.common;

import it.polimi.affetti.tspoon.evaluation.UniquelyRepresentableForTracking;

import java.io.Serializable;

/**
 * Created by affo on 04/05/18.
 */
public class ComposedID
        implements UniquelyRepresentableForTracking, Serializable {
    public final long left;
    public final long right;

    public ComposedID() {
        this(-1, -1);
    }

    public ComposedID(long left, long right) {
        this.left = left;
        this.right = right;
    }

    public static ComposedID of(long left, long right) {
        return new ComposedID(left, right);
    }

    @Override
    public String toString() {
        return left + "." + right;
    }

    @Override
    public String getUniqueRepresentation() {
        return toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComposedID that = (ComposedID) o;

        if (left != that.left) return false;
        return right == that.right;
    }

    @Override
    public int hashCode() {
        int result = (int) (left ^ (left >>> 32));
        result = 31 * result + (int) (right ^ (right >>> 32));
        return result;
    }
}
