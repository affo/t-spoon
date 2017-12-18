package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by affo on 13/07/17.
 */
public class Enriched<T> implements Serializable {
    public final Metadata metadata;
    public final T value;

    public Enriched() {
        metadata = new Metadata();
        value = null;
    }

    public Enriched(Metadata meta, T value) {
        this.metadata = meta;
        this.value = value;
    }

    public static <R> Enriched<R> of(Metadata metadata, R value) {
        return new Enriched<>(metadata, value);
    }

    public <R> Enriched<R> replace(R value) {
        return Enriched.of(metadata, value);
    }

    @Override
    public String toString() {
        return "Enriched{" +
                "metadata=" + metadata +
                ", value=" + value +
                '}';
    }

    public static <T> TypeInformation<Enriched<T>> getTypeInfo(TypeInformation<T> typeInfo) {
        try {
            List<PojoField> fields = Arrays.asList(
                    new PojoField(Enriched.class.getField("metadata"),
                            TypeInformation.of(Metadata.class)),
                    new PojoField(Enriched.class.getField("value"), typeInfo)
            );

            Class<Enriched<T>> baseClass = (Class<Enriched<T>>) (Class<?>) Enriched.class;
            return new PojoTypeInfo<>(baseClass, fields);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Error in extracting type information from Enriched: " + e.getMessage());
        }
    }
}
