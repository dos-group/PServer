package de.tuberlin.pserver.app.types.experimental;

public class PagedDMatrixValue {/*extends AbstractBufferValue implements DMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements DMatrix.RowIterator {

        private PagedDMatrixValue self;

        private final int end;

        private int segmentIndex;

        private int localIndex;

        private int globalRowIndex;

        private double[] currentSegment;

        private double[] nextSegment;

        private final int startRow;

        // ---------------------------------------------------

        public RowIterator(final PagedDMatrixValue v) { this(v, 0, (int)Preconditions.checkNotNull(v).numRows()); }
        public RowIterator(final PagedDMatrixValue v, final int startRow, final int endRow) {

            this.self = Preconditions.checkNotNull(v);
            Preconditions.checkArgument(startRow >= 0 && startRow < self.numRows());

            this.startRow       = startRow;
            this.end            = endRow * self.cols;
            this.segmentIndex   = startRow / (self.segmentSize / Double.BYTES);
            this.globalRowIndex = startRow - self.cols;
            this.localIndex     = (globalRowIndex % (self.segmentSize / Double.BYTES));

            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return globalRowIndex < end; }

        @Override
        public void nextRow() {
            globalRowIndex += self.cols;
            localIndex  += self.cols;
            if (localIndex >= currentSegment.length)
                nextSegment();
        }

        @Override
        public double getValueOfColumn(final int col) {
            if (localIndex + col >= currentSegment.length)
                return nextSegment[(localIndex + col) - currentSegment.length];
            else
                return currentSegment[localIndex + col];
        }

        @Override
        public void reset() {
            segmentIndex = startRow / (self.segmentSize / Double.BYTES);
            globalRowIndex = startRow - self.cols;
            localIndex   = (globalRowIndex % (self.segmentSize / Double.BYTES));

            currentSegment = self.buffers.get(segmentIndex);
            if (self.buffers.length() > segmentIndex + 1)
                nextSegment = self.buffers.get(segmentIndex + 1);

            //currentSegment = self.buffers.get(0);
            //if (self.buffers.length() >= 1)
            //    nextSegment = self.buffers.get(1);
            //globalRowIndex = startRow - self.cols;
            //localIndex   = 0;
            //segmentIndex = 0;
        }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }

        // ---------------------------------------------------

        private void nextSegment() {
            ++segmentIndex;
            currentSegment = nextSegment;
            if (segmentIndex + 1 < self.buffers.length())
                nextSegment = self.buffers.get(segmentIndex + 1);
            localIndex = localIndex - currentSegment.length;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1;

    //private static final Logger LOG = LoggerFactory.getLogger(PagedDoubleBufferValue.class);

    protected final List<double[]> buffers;

    protected final int segmentSize;

    protected final int rows;

    protected final int cols;

    protected final MemoryLayout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PagedDMatrixValue(final int rows, final int cols, final List<double[]> buffers) {
        this(rows, cols, buffers, MemoryLayout.ROW_LAYOUT);
    }

    public PagedDMatrixValue(final int rows, final int cols, final List<double[]> buffers, MemoryLayout layout) {
        super(rows * cols * Double.BYTES, false);

        this.rows           = rows;
        this.cols           = cols;
        this.layout         = Preconditions.checkNotNull(layout);
        this.segmentSize    = MemoryManager.DEFAULT_MEMORY_SEGMENT_SIZE;//MemoryManager.getMemoryManager().getSegmentSize();
        this.buffers        = Preconditions.checkNotNull(buffers);

        //LOG.info("Created PagedDoubleBufferValue: rows = " + rows + ", cols = " + cols
        //        + ", partitionSize = " + getPartitionSize() + ", buffers = " + buffers.length());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void compress() { throw new UnsupportedOperationException(); }

    public void decompress() { throw new UnsupportedOperationException(); }

    public void allocateMemory(final int instanceID) {}

    public Segment[] getSegments(int[] segmentIndices, int instanceID) { throw new UnsupportedOperationException(); }

    public void putSegments(Segment[] segments, int instanceID) { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------

    @Override
    public double get(final long row, final long col) {
        final int globalOffset = getPos(row, col) * Double.BYTES;
        final int segmentIdx = globalOffset / segmentSize;
        final int localOffset = globalOffset % segmentSize;
        return buffers.get(segmentIdx)[localOffset];
    }

    @Override
    public void set(final long row, final long col, final double value) {
        final int globalOffset = getPos(row, col) * Double.BYTES;
        final int segmentIdx = globalOffset / segmentSize;
        final int localOffset = globalOffset % segmentSize;
        buffers.get(segmentIdx)[localOffset] = value;
    }

    @Override
    public long numRows() { return rows; }

    @Override
    public long numCols() { return cols; }

    @Override
    public DMatrix.RowIterator rowIterator(final int startRow, final int endRow) { return new RowIterator(this, startRow, endRow); }

    @Override
    public DMatrix.RowIterator rowIterator() { return new RowIterator(this); }

    @Override
    public double[] toArray() { throw new UnsupportedOperationException(); }

    @Override
    public void setArray(double[] data) { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public DMatrix add(final DMatrix B) { throw new UnsupportedOperationException(); }

    @Override public DMatrix sub(final DMatrix B) { throw new UnsupportedOperationException(); }

    @Override public DVector mul(final DVector x, final DVector y) { throw new UnsupportedOperationException(); }

    @Override public DMatrix scale(final double alpha) { throw new UnsupportedOperationException(); }

    @Override public DMatrix transpose() { throw new UnsupportedOperationException(); }

    @Override public DMatrix transpose(final DMatrix B) { throw new UnsupportedOperationException(); }

    @Override public boolean invert() { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private int getPos(final long row, final long col) {
        switch (layout) {
            case ROW_LAYOUT: return (int)(row * cols + col);
            case COLUMN_LAYOUT: return (int)(col * rows + row);
        }
        throw new IllegalStateException();
    }*/
}
