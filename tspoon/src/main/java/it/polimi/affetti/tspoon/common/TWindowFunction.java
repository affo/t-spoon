package it.polimi.affetti.tspoon.common;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.List;

/**
 * Created by affo on 07/11/17.
 */
public interface TWindowFunction<I, O> extends Function, Serializable {
    O apply(List<I> batch);
}
