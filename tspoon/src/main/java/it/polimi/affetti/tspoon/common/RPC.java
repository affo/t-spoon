package it.polimi.affetti.tspoon.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by affo on 23/03/18.
 */
public class RPC {
    public String methodName;
    public List<Object> params;

    public RPC() {
    }

    public RPC(String methodName, Object... params) {
        this.methodName = methodName;
        this.params = new ArrayList<>(Arrays.asList(params));
    }

    public void addParam(Object param) {
        params.add(param);
    }

    public Object call(Object anObject) throws InvocationTargetException, IllegalAccessException {
        Method[] methods = anObject.getClass().getMethods();
        Method spc = null;

        for (Method m : methods) {
            if (m.getName().equals(methodName) && m.getAnnotationsByType(SinglePartitionCommand.class).length > 0) {
                spc = m;
            }
        }

        if (spc == null) {
            throw new IllegalArgumentException("No method found for spc " + toString());
        }

        if (!spc.isAccessible()) {
            spc.setAccessible(true);
        }

        return spc.invoke(anObject, params.toArray());
    }

    @Override
    public String toString() {
        return methodName + params;
    }
}
