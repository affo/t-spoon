package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;

/**
 * Created by affo on 02/12/17.
 */
public class SynchronousTwoPCTest extends SimpleTwoPCTest {

    @Override
    protected void configureTransactionalEnvironment(TransactionEnvironment tEnv) {
        tEnv.setSynchronous(true);
    }
}
