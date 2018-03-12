package it.polimi.affetti.tspoon.common;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by affo on 10/03/18.
 */
public class PartitionOrBcastPartitioner<T extends PartitionOrBcastPartitioner.PartitionAwareRecord, K>
        extends StreamPartitioner<T> {
    private final KeyGroupStreamPartitioner<K, K> keyByPartitioner;
    private final BroadcastPartitioner<T> broadcastPartitioner;

    private transient SerializationDelegate<StreamRecord<K>> delegateReuseForNextKey;
    private transient StreamRecord<K> srReuseForNextKey;

    // save memory with arrays
    private BitSet interestedChannels;
    private Map<Integer, int[]> arrays = new HashMap<>();

    private PartitionOrBcastPartitioner() {
        KeySelector<K, K> keySelector = k -> k; // identity function
        this.keyByPartitioner = new KeyGroupStreamPartitioner<>(keySelector,
                StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        this.broadcastPartitioner = new BroadcastPartitioner<>();
    }

    public static <T extends PartitionAwareRecord> DataStream<T> apply(DataStream<T> ds) {
        MyDataStream<T> myDataStream = new MyDataStream<>(ds);
        return myDataStream.partitionOrBcast();
    }

    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int[] selectChannels(SerializationDelegate<StreamRecord<T>> sr, int numberOfOutputs) {
        updateArrays(numberOfOutputs);

        T value = sr.getInstance().getValue();
        if (value instanceof Broadcastable) {
            value.setNumberOfPartitions(numberOfOutputs);
            return broadcastPartitioner.selectChannels(sr, numberOfOutputs);
        }

        if (value instanceof PartitionOrBcastPartitioner.Partitionable) {
            Partitionable<K> partitionable = (Partitionable<K>) value;
            int[] channels = getChannelArray(partitionable, numberOfOutputs);
            partitionable.setNumberOfPartitions(channels.length);
            return channels;
        }

        throw new RuntimeException("Cannot partition or broadcast element");
    }

    private void updateArrays(int numberOfOutputs) {
        if (interestedChannels == null || interestedChannels.size() != numberOfOutputs) {
            interestedChannels = new BitSet(numberOfOutputs);
        } else {
            interestedChannels.clear();
        }

        if (numberOfOutputs > arrays.size()) {
            for (int i = arrays.size() + 1; i <= numberOfOutputs; i++) {
                arrays.put(i, new int[i]);
            }
        }
    }

    private void setAsNextKey(K key) {
        if (srReuseForNextKey == null || delegateReuseForNextKey == null) {
            srReuseForNextKey = new StreamRecord<>(null);
            // we pass null because we know that the keyPartitioner
            // will not use the serializer
            delegateReuseForNextKey = new SerializationDelegate<>(null);
        }

        StreamRecord<K> replaced = srReuseForNextKey.replace(key);
        delegateReuseForNextKey.setInstance(replaced);
    }

    private int[] getChannelArray(Partitionable<K> partitionable, int numberOfOutputs) {
        for (K key : partitionable.getKeys()) {
            setAsNextKey(key);
            int channel = keyByPartitioner.selectChannels(delegateReuseForNextKey, numberOfOutputs)[0];
            interestedChannels.set(channel);
        }

        int[] channelArray = arrays.get(interestedChannels.cardinality());
        int j = 0;

        for (int i = interestedChannels.nextSetBit(0); i >= 0;
             i = interestedChannels.nextSetBit(i + 1)) {
            channelArray[j++] = i;
        }

        return channelArray;
    }

    private static class MyDataStream<T extends PartitionAwareRecord> extends DataStream<T> {
        public MyDataStream(DataStream<T> wrapped) {
            super(wrapped.getExecutionEnvironment(), wrapped.getTransformation());
        }

        private DataStream<T> partitionOrBcast() {
            return setConnectionType(new PartitionOrBcastPartitioner<>());
        }
    }

    public interface PartitionAwareRecord {
        void setNumberOfPartitions(int n);
    }

    public interface Partitionable<K> extends PartitionAwareRecord {
        Set<K> getKeys();
    }

    public interface Broadcastable extends PartitionAwareRecord {
    }
}
