package de.tuberlin.pserver.math;


import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import no.uib.cipr.matrix.sparse.SparseVector;

public class SVector extends AbstractVector {

    protected SparseVector data;

    private static final LibraryVectorOps<Vector> vectorOpDelegate =
            MathLibFactory.delegateSVectorOpsTo(MathLibFactory.SMathLibrary.MTJ_LIBRARY);

    public SVector(long size, int[] index, double[] data, VectorType type) {
        super(size, type);
        this.data = new SparseVector(Utils.toInt(size), index, data);
    }

    public SVector(long size, VectorType type) {
        super(size, type);
        this.data = new SparseVector(Utils.toInt(size));
    }

    public SVector(long size) {
        super(size, VectorType.ROW_VECTOR);
        this.data = new SparseVector(Utils.toInt(size));
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
    public double atomicGet(long index) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void atomicSet(long index, double value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector viewPart(long s, long e) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector like() {
        return new SVector(size, data.getIndex(), data.getData(), type);
    }

    @Override
    public Vector copy() {
        throw new UnsupportedOperationException("");
    }

    public SparseVector getContainer() {
        return data;
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Vector mul(final double alpha) { return vectorOpDelegate.mul(this, alpha); }

    @Override public Vector div(final double alpha) { return vectorOpDelegate.div(this, alpha); }

    @Override public Vector add(final Vector y) { return vectorOpDelegate.add(this, y); }

    @Override public Vector sub(final Vector y) { return vectorOpDelegate.sub(this, y); }

    @Override public Vector add(final double alpha, final Vector y) { return vectorOpDelegate.add(this, alpha, y); }

    @Override public double dot(final Vector y) { return vectorOpDelegate.dot(this, y); }

    @Override public double norm(final double power) { return vectorOpDelegate.norm(this, power); }
}
