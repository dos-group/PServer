package de.tuberlin.pserver.app.types;

import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Vector;

public class DVectorValue extends AbstractBufferValue {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private double[] data;

    public final Vector vector;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DVectorValue(final long size,
                        final boolean allocateMemory) {

        super((int)(size * Double.BYTES), allocateMemory);
        data = new double[(int)size];
        vector = new DVector(size, data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void compress() { throw new UnsupportedOperationException(); }

    @Override
    public void decompress() { throw new UnsupportedOperationException(); }

    @Override
    public void allocateMemory(final int instanceID) {
        /*Preconditions.checkState(data == null);
        final byte[] byteData = new byte[Preconditions.checkNotNull(key).getPartitionDescriptor(instanceID).partitionSize];
        data = UnsafeOp.primitiveArrayTypeCast(byteData, byte[].class, double[].class);*/
    }

    @Override
    public Segment[] getSegments(final int[] segmentIndices, final int instanceID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putSegments(final Segment[] segments, final int instanceID) {
        throw new UnsupportedOperationException();
    }
}