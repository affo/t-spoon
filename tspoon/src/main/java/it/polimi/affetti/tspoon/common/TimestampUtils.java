package it.polimi.affetti.tspoon.common;

/**
 * Created by affo on 27/04/18.
 */
public class TimestampUtils {
    // the multiplier to create/check timestamps
    private static int multiplier;
    private static long baseTime;
    private static boolean init = false;

    public synchronized static void init(int numberOfSources) {
        if (!init) {
            int suffixLength = String.valueOf(numberOfSources).length();
            multiplier = (int) Math.pow(10, suffixLength);
            baseTime = System.nanoTime();
            init = true;
        }
    }

    public static long getTimestamp(int sourceID) {
        long ts = System.nanoTime() - baseTime;// I'm lovin' it :)
        ts *= multiplier;
        ts += sourceID;
        return ts;
    }

    public static boolean checkTimestamp(int sourceID, long ts) {
        return ts % multiplier == sourceID;
    }

    public static boolean sameSource(long ts1, long ts2) {
        return ts1 % multiplier == ts2 % multiplier;
    }
}
