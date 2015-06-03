package de.tuberlin.pserver.app.types;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.math.DMatrix;
import de.tuberlin.pserver.math.Matrix;

public class DMatrixValue extends AbstractBufferValue {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private double[] data;

    public final Matrix matrix;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DMatrixValue(final int rows,
                        final int cols,
                        final boolean allocateMemory,
                        final DMatrix.MemoryLayout layout) {

        super(rows * cols * Double.BYTES, allocateMemory);
        this.data = new double[rows * cols];
        this.matrix = new DMatrix(rows, cols, data, layout);
        this.matrix.setOwner(this);
    }

    public DMatrixValue(final int rows,
                        final int cols,
                        final double[] data) {

        super(rows * cols * Double.BYTES, false);
        this.data = Preconditions.checkNotNull(data);
        this.matrix = new DMatrix(rows, cols, data, DMatrix.MemoryLayout.ROW_LAYOUT);
        this.matrix.setOwner(this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void compress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void decompress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allocateMemory(int instanceID) {}

    @Override
    public Segment[] getSegments(int[] segmentIndices, int instanceID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putSegments(Segment[] segments, int instanceID) {
        throw new UnsupportedOperationException();
    }
}