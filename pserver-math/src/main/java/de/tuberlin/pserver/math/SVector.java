package de.tuberlin.pserver.math;


import no.uib.cipr.matrix.sparse.SparseVector;

public class SVector extends AbstractVector {

    protected SparseVector data;

    public SVector(long size, int[] index, double[] data, VectorType type) {
        super(size, type);
        this.data = new SparseVector(Utils.toInt(size), index, data);
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public void set(long index, double value) {
        data.set(Utils.toInt(index), value);
    }

    @Override
    public double get(long index) {
        return data.get(Utils.toInt(index));
    }

    @Override
    public Vector viewPart(long s, long e) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector like() {
        return new SVector(size, data.getIndex(), data.getData(), type);
    }

    public SparseVector getContainer() {
        return data;
    }
}
