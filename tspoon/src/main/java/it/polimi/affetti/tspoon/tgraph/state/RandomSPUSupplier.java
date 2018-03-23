package it.polimi.affetti.tspoon.tgraph.state;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by affo on 23/03/18.
 */
public interface RandomSPUSupplier extends Serializable {
    SinglePartitionUpdate next(SinglePartitionUpdateID spuID, Random random);
}
