package de.tuberlin.pserver.app.types;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.delegates.LibraryVectorOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;

import java.util.Arrays;

public class DVectorValue extends AbstractBufferValue implements DVector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final LibraryVectorOps<DVector> vectorOpDelegate =
            MathLibFactory.delegateDVectorOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    protected double[] data;

    protected long size;

    protected VectorType type;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DVectorValue(final long size,
                        final boolean allocateMemory,
                        final VectorType type) {

        super((int)(size * Double.BYTES), allocateMemory);
        this.size = size;
        this.type = type;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int size() { return (int)size; }

    @Override
    public void set(final int index, final double value) { data[index] = value; }

    @Override
    public double get(final int index) { return data[index]; }

    @Override
    public DVector zero() {
        Arrays.fill(data, 0);
        return this;
    }

    @Override
    public DVector set(final DVector y) {
        Preconditions.checkState(data.length == y.toArray().length);
        System.arraycopy(y.toArray(), 0, data, 0, data.length);
        return null;
    }

    @Override
    public VectorType getVectorType() {
        return type;
    }

    @Override
    public double[] toArray() { return data; }

    @Override
    public void setArray(final double[] data) { this.data = Preconditions.checkNotNull(data); }

    // ---------------------------------------------------

    @Override
    public void compress() { throw new UnsupportedOperationException(); }

    @Override
    public void decompress() { throw new UnsupportedOperationException(); }

    @Override
    public void allocateMemory(final int instanceID) {
        Preconditions.checkState(data == null);
        final byte[] byteData = new byte[Preconditions.checkNotNull(key).getPartitionDescriptor(instanceID).partitionSize];
        data = UnsafeOp.primitiveArrayTypeCast(byteData, byte[].class, double[].class);
    }

    @Override
    public Segment[] getSegments(final int[] segmentIndices, final int instanceID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putSegments(final Segment[] segments, final int instanceID) {
        throw new UnsupportedOperationException();
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public DVector scale(final double alpha) { return vectorOpDelegate.scale(this, alpha); }

    @Override public DVector add(final DVector y) { return vectorOpDelegate.add(this, y); }

    @Override public DVector add(final double alpha, final DVector y) { return vectorOpDelegate.add(this, alpha, y); }

    @Override public double dot(final DVector y) { return vectorOpDelegate.dot(this, y); }
}