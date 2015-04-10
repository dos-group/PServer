package de.tuberlin.pserver.app.dht.valuetypes;

import com.google.common.base.Preconditions;

public class DoubleBufferValue extends AbstractBufferValue{

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum BlockLayout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected double[] data;

    protected final int rows;

    protected final int cols;

    protected final BlockLayout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DoubleBufferValue(final int rows,
                             final int cols,
                             final boolean allocateMemory) {

        super(rows * cols * Double.BYTES, allocateMemory);

        this.rows   = rows;
        this.cols   = cols;
        this.layout = BlockLayout.ROW_LAYOUT;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long numRows() { return rows; }

    public long numCols() { return cols; }

    public double[] toDoubleArray() { return data; }

    // ---------------------------------------------------

    public double get(final int row, final int col) { return data[getPos(row, col)]; }

    public void set(final int row, final int col, final double value) { data[getPos(row, col)] = value; }

    // ---------------------------------------------------

    @Override
    public void compress() { throw new UnsupportedOperationException(); }

    @Override
    public void decompress() { throw new UnsupportedOperationException(); }

    @Override
    public void allocateMemory(final int instanceID) {
        Preconditions.checkState(data == null);
        data = new double[Preconditions.checkNotNull(key).getPartitionDescriptor(instanceID).partitionSize / Double.BYTES];

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
    // Public Methods.
    // ---------------------------------------------------

    protected int getPos(final int row, final int col) {
        switch (layout) {
            case ROW_LAYOUT: return (row * cols + col);
            case COLUMN_LAYOUT: return (col * rows + row);
        }
        throw new IllegalStateException();
    }
}
