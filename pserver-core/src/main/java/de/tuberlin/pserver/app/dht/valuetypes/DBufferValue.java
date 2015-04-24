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

        private int globalRowIndex;

        private final int startRow;

        // ---------------------------------------------------

        public RowIterator(final DBufferValue v) { this(v, 0, (int)Preconditions.checkNotNull(v).numRows()); }
        public RowIterator(final DBufferValue v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());
            Preconditions.checkArgument(endRow >= 0 && endRow < self.numRows());
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
    public DMatrix.RowIterator rowIterator(final int startRow, final int endRow) { return new RowIterator(this, startRow, endRow); }

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
