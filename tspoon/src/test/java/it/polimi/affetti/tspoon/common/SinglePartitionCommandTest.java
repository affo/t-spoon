package it.polimi.affetti.tspoon.common;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by affo on 23/03/18.
 */
public class SinglePartitionCommandTest {

    @Test
    public void testSinglePartitionCommandAnnotation() throws InvocationTargetException, IllegalAccessException {
        RPC rpc = new RPC("apply", "lol", 1, 42);
        AClass anObject = new AClass();

        Assert.assertFalse(anObject.isCalled());
        rpc.call(anObject);
        Assert.assertTrue(anObject.isCalled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongCall() throws InvocationTargetException, IllegalAccessException {
        RPC rpc = new RPC("notSpc");
        AClass anObject = new AClass();

        rpc.call(anObject);
    }

    private static class AClass {
        private boolean called = false;

        @SinglePartitionCommand
        public void apply(String aString, Integer anInteger, int anInt) {
            called = true;
            System.out.printf("I was called with (%s, %d, %d)\n", aString, anInteger, anInt);
        }

        public void notSpc() {
            // does nothing
        }

        public boolean isCalled() {
            return called;
        }
    }
}
