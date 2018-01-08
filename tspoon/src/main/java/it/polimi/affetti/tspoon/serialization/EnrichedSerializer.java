package it.polimi.affetti.tspoon.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;

import java.io.Serializable;

/**
 * Created by affo on 05/01/18.
 */
public class EnrichedSerializer extends Serializer<Enriched<?>> implements Serializable {
    @Override
    public void write(Kryo kryo, Output output, Enriched<?> enriched) {
        // TODO remove
        System.out.println("WRITING with my own serializers");

        kryo.writeObject(output, enriched.metadata);
        kryo.writeClassAndObject(output, enriched.value);
    }

    @Override
    public Enriched<?> read(Kryo kryo, Input input, Class<Enriched<?>> aClass) {
        // TODO remove
        System.out.println("READING with my own serializers");

        Metadata metadata = kryo.readObject(input, Metadata.class);
        Object value = kryo.readClassAndObject(input);
        return Enriched.of(metadata, value);
    }
}
