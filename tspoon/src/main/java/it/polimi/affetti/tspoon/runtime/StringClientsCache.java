package it.polimi.affetti.tspoon.runtime;

/**
 * Created by affo on 24/07/17.
 */
public class StringClientsCache extends ClientsCache<StringClient> {
    public StringClientsCache() {
        super(a -> new StringClient(a.ip, a.port));
    }
}
