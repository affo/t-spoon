package it.polimi.affetti.tspoon.common;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.List;

/**
 * Created by affo on 20/12/16.
 */
public interface FlatMapFunction<I, O> extends Function, Serializable {
    List<O> flatMap(I input) throws Exception;
}
