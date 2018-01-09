package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.common.RandomProvider;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Random;

/**
 * Created by affo on 26/07/17.
 */
public class Transfer extends Tuple4<TransferID, String, String, Double> {
    private static Random random = RandomProvider.get();

    public Transfer() {
    }

    public Transfer(TransferID id, String from, String to, Double amount) {
        super(id, from, to, amount);
    }

    public Movement getDeposit() {
        return new Movement(this.f0, this.f1, -this.f3);
    }

    public Movement getWithdrawal() {
        return new Movement(this.f0, this.f2, this.f3);
    }

    public static Transfer generateTransfer(TransferID id, int noAccounts, double startAmount) {
        String from = "a" + random.nextInt(noAccounts);
        String to;
        do {
            to = "a" + random.nextInt(noAccounts);
        } while (from.equals(to));
        Double amount = Math.ceil(random.nextDouble() * startAmount);

        return new Transfer(id, from, to, amount);
    }
}
