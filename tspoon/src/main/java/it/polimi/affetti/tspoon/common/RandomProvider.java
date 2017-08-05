package it.polimi.affetti.tspoon.common;

import java.util.Random;

public class RandomProvider {
    public static final int seed = 0;

    public static Random get() {
        return new Random(seed);
    }
}
