package de.tuberlin.pserver.math.vector.sparse;

import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.vector.AbstractVector;
import de.tuberlin.pserver.math.vector.Vector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SVector extends AbstractVector {

    private final Map<Long, Double> data = new HashMap<>();

    public SVector(long length, Layout type) {
        super(length, type);
    }

    @Override
    public Format format() {
        return Format.SPARSE_FORMAT;
    }

    @Override
    public void set(long index, double value) {
        data.put(index, value);
    }

    @Override
    public double get(long index) {
        Double result = data.get(index);
        if (result == null) {
            return 0.0;
        }
        return result;
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
    public Vector concat(Vector v) {
        return null;
    }

    @Override
    public Vector concat(List<Vector> vList) {
        return null;
    }

    @Override
    public ElementIterator nonZeroElementIterator() {
        return null;
    }

    @Override
    public ElementIterator elementIterator() {
        return null;
    }

    @Override
    public ElementIterator elementIterator(int start, int end) {
        return null;
    }

    @Override
    public double[] toArray() {
        double[] result = new double[(int)length];
        for (int i = 0; i < result.length; i++) {
            result[i] = data.get(i);
        }
        return result;
    }

    @Override
    protected Vector newInstance(long length) {
        return new SVector(length, type);
    }
}
