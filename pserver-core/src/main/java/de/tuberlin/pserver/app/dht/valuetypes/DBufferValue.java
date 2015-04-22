package de.tuberlin.pserver.app.dht.valuetypes;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.types.DMatrix;
import de.tuberlin.pserver.utils.UnsafeOp;

public class DBufferValue extends AbstractBufferValue implements DMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements DMatrix.RowIterator {

        private DBufferValue self;

        private final long end;

        private int globalIndex;

        // ---------------------------------------------------

        public RowIterator(final DBufferValue s) {
            this.self = Preconditions.checkNotNull(s);
            this.end  = self.rows * self.cols;
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return globalIndex < end; }

        @Override
        public void nextRow() { globalIndex += self.cols; }

        @Override
        public double getValue(final int col) { return self.data[globalIndex + col]; }

        @Override
        public void reset() { globalIndex  = 0; }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected double[] data;

    protected final long rows;

    protected final long cols;

    protected final MemoryLayout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DBufferValue(final int rows,
                        final int cols,
                        final boolean allocateMemory,
                        final MemoryLayout layout) {

        super(rows * cols * Double.BYTES, allocateMemory);

        this.rows   = rows;
        this.cols   = cols;
        this.layout = Preconditions.checkNotNull(layout);
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
    public DMatrix.RowIterator rowIterator() { return new RowIterator(this); }

    @Override
    public double[] toArray() { return data; }

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
