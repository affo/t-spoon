package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by affo on 27/02/18.
 *
 * Gathers the updates for a transaction in a transactional graph
 */
public class Updates implements Serializable, Cloneable {
    private final Map<Key, Object> updates = new HashMap<>();
    // view of the namespaces in this update
    private final Set<String> namespacesInvolved = new HashSet<>();

    public void addUpdate(String namespace, String key, Object update) {
        Key k = new Key(namespace, key);
        updates.put(k, update);
        namespacesInvolved.add(namespace);
    }

    public boolean isInvolved(String namespace) {
        return namespacesInvolved.contains(namespace);
    }

    @SuppressWarnings("unchecked")
    public <T> T getUpdate(String namespace, String key) {
        return (T) updates.get(new Key(namespace, key));
    }

    @SuppressWarnings("unchecked")
    public <T> Map<String, T> getUpdatesFor(String namespace) {
        return updates.entrySet().stream()
                .filter(entry -> entry.getKey().namespace.equals(namespace))
                .map(entry -> Tuple2.of(entry.getKey().key, (T) entry.getValue()))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    @Override
    public Updates clone() {
        Updates cloned = new Updates();
        cloned.updates.putAll(this.updates);
        cloned.namespacesInvolved.addAll(namespacesInvolved);
        return cloned;
    }

    public void clear() {
        updates.clear();
    }

    /**
     * NOTE: side effects on this
     */
    public void merge(Updates other) {
        updates.putAll(other.updates);
        namespacesInvolved.addAll(other.namespacesInvolved);
    }

    @Override
    public String toString() {
        return updates.toString();
    }

    private static final class Key implements Serializable {
        public final String namespace, key;

        private Key(String namespace, String key) {
            this.namespace = namespace;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key1 = (Key) o;

            if (namespace != null ? !namespace.equals(key1.namespace) : key1.namespace != null) return false;
            return key != null ? key.equals(key1.key) : key1.key == null;
        }

        @Override
        public int hashCode() {
            int result = namespace != null ? namespace.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return namespace + "." + key;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Updates updates1 = (Updates) o;

        return updates != null ? updates.equals(updates1.updates) : updates1.updates == null;
    }

    @Override
    public int hashCode() {
        return updates != null ? updates.hashCode() : 0;
    }
}
