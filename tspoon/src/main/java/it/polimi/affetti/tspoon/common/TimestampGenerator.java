package it.polimi.affetti.tspoon.common;

import org.apache.flink.util.Preconditions;

/**
 * Created by affo on 27/04/18.
 */
public class TimestampGenerator {
    private long startTs;
    private final int unit, sourceNumber;

    /**
     * @param numberOfSources
     * @param startTs must end with the source id
     */
    public TimestampGenerator(int sourceNumber, int numberOfSources, long startTs) {
        this.sourceNumber = sourceNumber;
        this.unit = calcUnit(numberOfSources);
        this.startTs = startTs;

        Preconditions.checkArgument(startTs % unit == sourceNumber);
    }

    public long getTimestamp() {
        return startTs;
    }

    public long nextTimestamp() {
        startTs += unit;
        return startTs;
    }

    public long toLogical(long realTimestamp) {
        return realTimestamp / unit;
    }

    public long toReal(long logicalTimestamp) {
        return logicalTimestamp * unit + sourceNumber;
    }

    public boolean checkTimestamp(int sourceID, long ts) {
        return checkTimestamp(sourceID, ts, unit);
    }

    public static int calcUnit(int numberOfSources) {
        return (int) Math.pow(10, String.valueOf(numberOfSources - 1).length());
    }

    public static boolean checkTimestamp(int sourceID, long ts, int unit) {
        return ts % unit == sourceID;
    }

    public static boolean sameSource(long ts1, long ts2, int unit) {
        return ts1 % unit == ts2 % unit;
    }
}
