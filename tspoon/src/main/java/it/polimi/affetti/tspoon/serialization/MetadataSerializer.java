package it.polimi.affetti.tspoon.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.tgraph.BatchID;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.Serializable;

/**
 * Created by affo on 05/01/18.
 */
public class MetadataSerializer extends Serializer<Metadata> implements Serializable {
    @Override
    public void write(Kryo kryo, Output output, Metadata metadata) {
        // transaction management
        kryo.writeObject(output, metadata.batchID);
        output.writeInt(metadata.timestamp);
        output.writeInt(metadata.watermark);
        output.writeInt(metadata.vote.ordinal());
        output.writeInt(metadata.dependencyTracking.size());
        for (Integer dependency : metadata.dependencyTracking) {
            output.writeInt(dependency);
        }

        // 2pc addresses
        kryo.writeObject(output, metadata.coordinator);
        output.writeInt(metadata.cohorts.size());
        for (Address cohort : metadata.cohorts) {
            kryo.writeObject(output, cohort);
        }
    }

    @Override
    public Metadata read(Kryo kryo, Input input, Class<Metadata> aClass) {
        Metadata metadata = new Metadata();
        metadata.batchID = kryo.readObject(input, BatchID.class);
        metadata.tid = metadata.batchID.getTid();
        metadata.timestamp = input.readInt();
        metadata.watermark = input.readInt();
        metadata.vote = Vote.values()[input.readInt()];
        int size = input.readInt();
        for (int i = 0; i < size; i++) {
            metadata.dependencyTracking.add(input.readInt());
        }

        metadata.coordinator = kryo.readObject(input, Address.class);
        int noCohorts = input.readInt();
        for (int i = 0; i < noCohorts; i++) {
            metadata.cohorts.add(kryo.readObject(input, Address.class));
        }

        return metadata;
    }
}
