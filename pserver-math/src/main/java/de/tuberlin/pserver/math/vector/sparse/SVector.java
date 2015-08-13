package de.tuberlin.pserver.math.vector.sparse;


import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.math.vector.AbstractVector;
import de.tuberlin.pserver.math.vector.Vector;
import no.uib.cipr.matrix.sparse.SparseVector;

public class SVector extends AbstractVector {

    protected SparseVector data;

    private static final LibraryVectorOps<Vector> vectorOpDelegate =
            MathLibFactory.delegateSVectorOpsTo(MathLibFactory.SMathLibrary.MTJ_LIBRARY);

    public SVector(long size, int[] index, double[] data, Layout type) {
        super(size, type);
        this.data = new SparseVector(Utils.toInt(size), index, data);
    }

    public SVector(long size, Layout type) {
        super(size, type);
        this.data = new SparseVector(Utils.toInt(size));
    }

    public SVector(long size) {
        super(size, Layout.ROW_LAYOUT);
        this.data = new SparseVector(Utils.toInt(size));
    }

    @Override public Format format() {
        return Format.SPARSE_FORMAT;
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
        return new SVector(length, data.getIndex(), data.getData(), type);
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

    @Override public Vector mul(final double alpha, final Vector y) { return vectorOpDelegate.mul(this, alpha, y); }

    @Override public Vector div(final double alpha, final Vector y) { return vectorOpDelegate.div(this, alpha, y); }

    @Override public Vector add(final Vector y, final Vector z) { return vectorOpDelegate.add(this, y, z); }

    @Override public Vector sub(final Vector y, final Vector z) { return vectorOpDelegate.sub(this, y, z); }

    @Override public Vector add(final double alpha, final Vector y, final Vector z) { return vectorOpDelegate.add(this, alpha, y, z); }

    @Override public double dot(final Vector y) { return vectorOpDelegate.dot(this, y); }

    @Override public double norm(final double power) { return vectorOpDelegate.norm(this, power); }

    @Override
    protected Vector newInstance(long length) {
        return new SVector(length, type);
    }
}
