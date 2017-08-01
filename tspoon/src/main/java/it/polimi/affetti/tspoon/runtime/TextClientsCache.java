package it.polimi.affetti.tspoon.runtime;

/**
 * Created by affo on 24/07/17.
 */
public class TextClientsCache extends ClientsCache<TextClient> {
    public TextClientsCache() {
        super(a -> new TextClient(a.ip, a.port));
    }
}
