package de.tuberlin.pserver.app.types;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.math.DMatrix;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DMatrixValue extends AbstractBufferValue implements DMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements DMatrix.RowIterator {

        private DMatrixValue self;

        private final long end;

        private int globalRowIndex;

        private final int startRow;

        // ---------------------------------------------------

        public RowIterator(final DMatrixValue v) { this(v, 0, (int)Preconditions.checkNotNull(v).numRows() - 1); }
        public RowIterator(final DMatrixValue v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());
            Preconditions.checkArgument(endRow > startRow && endRow < self.numRows());
            this.startRow = startRow;
            this.end = endRow * self.cols;
            this.globalRowIndex = startRow - (int)-self.cols;
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return globalRowIndex < end || globalRowIndex < self.rows * self.cols; }

        @Override
        public void nextRow() { globalRowIndex += self.cols; }

        @Override
        public double getValueOfColumn(final int col) { return self.data[globalRowIndex + col]; }

        @Override
        public void reset() { globalRowIndex = startRow - (int)self.cols; }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DMatrixValue.class);

    private static final LibraryMatrixOps<DMatrix, DVector> matrixOpDelegate =
            MathLibFactory.delegateDMatrixOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    protected double[] data;

    protected final long rows;

    protected final long cols;

    protected final MemoryLayout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DMatrixValue(final int rows,
                        final int cols,
                        final boolean allocateMemory,
                        final MemoryLayout layout) {

        super(rows * cols * Double.BYTES, allocateMemory);

        this.rows   = rows;
        this.cols   = cols;
        this.layout = Preconditions.checkNotNull(layout);
    }

    public DMatrixValue(final int rows,
                        final int cols,
                        final double[] data) {

        super(rows * cols * Double.BYTES, false);

        this.rows   = rows;
        this.cols   = cols;
        this.layout = MemoryLayout.ROW_LAYOUT;
        this.data   = Preconditions.checkNotNull(data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long numRows() { return rows; }

    @Override
    public long numCols() { return cols; }

    @Override
    public double get(final long row, final long col) { return data[getPos(row, col)]; }

    @Override
    public void set(final long row, final long col, final double value) { data[getPos(row, col)] = value; }

    @Override
    public DMatrix.RowIterator rowIterator(final int startRow, final int endRow) { return new RowIterator(this, startRow, endRow); }

    @Override
    public DMatrix.RowIterator rowIterator() { return new RowIterator(this); }

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
        if (data == null) {
            final byte[] byteData = new byte[Preconditions.checkNotNull(key).getPartitionDescriptor(instanceID).partitionSize];
            data = UnsafeOp.primitiveArrayTypeCast(byteData, byte[].class, double[].class);
        }
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

    @Override public DMatrix add(final DMatrix B) { return matrixOpDelegate.add(B, this); }

    @Override public DMatrix sub(final DMatrix B) { return matrixOpDelegate.sub(B, this); }

    @Override public DVector mul(final DVector x, final DVector y) { return matrixOpDelegate.mul(this, x, y); }

    @Override public DMatrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    @Override public DMatrix transpose() { return matrixOpDelegate.transpose(this); }

    @Override public DMatrix transpose(final DMatrix B) { return matrixOpDelegate.transpose(B, this); }

    @Override public boolean invert() { return matrixOpDelegate.invert(this); }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private int getPos(final long row, final long col) {
        switch (layout) {
            case ROW_LAYOUT: return (int)(row * cols + col);
            case COLUMN_LAYOUT: return (int)(col * rows + row);
        }
        throw new IllegalStateException();
    }
}
