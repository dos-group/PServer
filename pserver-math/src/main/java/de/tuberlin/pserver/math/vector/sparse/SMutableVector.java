package de.tuberlin.pserver.math.vector.sparse;

import de.tuberlin.pserver.math.vector.AbstractVector;
import de.tuberlin.pserver.math.vector.Vector;

import java.util.HashMap;
import java.util.Map;

public class SMutableVector extends AbstractVector {

    private final Map<Long, Double> data = new HashMap<>();

    public SMutableVector(long length, Layout type) {
        super(length, type);
    }

    @Override
    public Format format() {
        return Format.SPARSE_VECTOR;
    }

    @Override
    public void set(long index, double value) {
        data.put(index, value);
    }

    @Override
    public double get(long index) {
        Double result = data.get(index);
        if(result == null) {
            return 0.0;
        }
        return result;
    }

    @Override
    public double atomicGet(long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void atomicSet(long index, double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vector viewPart(long s, long e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vector like() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vector copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double[] toArray() {
        double[] result = new double[(int)length];
        for (int i = 0; i < result.length; i++) {
            result[i] = data.get(i);
        }
        return result;
    }
}
