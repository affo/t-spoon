package it.polimi.affetti.tspoon.tgraph.backed;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by affo on 26/07/17.
 */
public class Transfer extends Tuple4<Long, String, String, Double> {
    public Transfer() {
    }

    public Transfer(Long id, String from, String to, Double amount) {
        super(id, from, to, amount);
    }

    public Movement getDeposit() {
        return new Movement(this.f0, this.f1, -this.f3);
    }

    public Movement getWithdrawal() {
        return new Movement(this.f0, this.f2, this.f3);
    }

}
