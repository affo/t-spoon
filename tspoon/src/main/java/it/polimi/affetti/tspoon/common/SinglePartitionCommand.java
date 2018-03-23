package it.polimi.affetti.tspoon.common;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
/**
 * Created by affo on 23/03/18.
 */
public @interface SinglePartitionCommand {
}
