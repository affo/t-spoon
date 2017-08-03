package it.polimi.affetti.tspoon.common;

import java.io.Serializable;

/**
 * Created by affo on 13/07/17.
 */
public class Address implements Serializable {
    public final String ip;
    public final int port;

    public Address() {
        this.ip = "";
        this.port = 0;
    }

    private Address(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public static Address of(String ip, int port) {
        return new Address(ip, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Address address = (Address) o;

        if (port != address.port) return false;
        return ip != null ? ip.equals(address.ip) : address.ip == null;
    }

    @Override
    public int hashCode() {
        int result = ip != null ? ip.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
