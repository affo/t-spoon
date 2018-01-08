package it.polimi.affetti.tspoon.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.polimi.affetti.tspoon.tgraph.BatchID;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * Created by affo on 05/01/18.
 */
public class BatchIDSerializer extends Serializer<BatchID> implements Serializable {
    @Override
    public void write(Kryo kryo, Output output, BatchID batchID) {
        output.writeInt(batchID.getNumberOfSteps());
        for (Tuple2<Integer, Integer> couple : batchID) {
            output.writeInt(couple.f0);
            output.writeInt(couple.f1);
        }
    }

    @Override
    public BatchID read(Kryo kryo, Input input, Class<BatchID> aClass) {
        int size = input.readInt();
        BatchID bid = new BatchID();
        for (int i = 0; i < size; i++) {
            bid.offsets.add(input.readInt());
            bid.sizes.add(input.readInt());
        }

        return bid;
    }
}
