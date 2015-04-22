package de.tuberlin.pserver.app.dht.valuetypes;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.memmng.MemoryManager;
import de.tuberlin.pserver.app.types.DMatrix;

import java.util.List;

public class PagedDBufferValue extends AbstractBufferValue implements DMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements DMatrix.RowIterator {

        private PagedDBufferValue self;

        private final int end;

        private int segmentIndex;

        private int localIndex;

        private int globalIndex;

        private double[] currentSegment;

        private double[] nextSegment;

        // ---------------------------------------------------

        public RowIterator(final PagedDBufferValue s) {
            this.self = Preconditions.checkNotNull(s);
            this.end  = self.rows * self.cols;
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNextRow() { return globalIndex < end; }

        @Override
        public void nextRow() {
            globalIndex += self.cols;
            localIndex  += self.cols;
            if (localIndex >= currentSegment.length)
                nextSegment();
        }

        @Override
        public double getValue(final int col) {
            if (localIndex + col >= currentSegment.length)
                return nextSegment[(localIndex + col) - currentSegment.length];
            else
                return currentSegment[localIndex + col];
        }

        @Override
        public void reset() {
            currentSegment = self.buffers.get(0);
            if (self.buffers.size() >= 1)
                nextSegment = self.buffers.get(1);
            localIndex   = 0;
            globalIndex  = 0;
            segmentIndex = 0;
        }

        @Override
        public long numRows() { return self.rows; }

        @Override
        public long numCols() { return self.cols; }

        // ---------------------------------------------------

        private void nextSegment() {
            ++segmentIndex;
            currentSegment = nextSegment;
            if (segmentIndex + 1 < self.buffers.size())
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

    public PagedDBufferValue(final int rows,  final int cols, final List<double[]> buffers) {
        this(rows, cols, buffers, MemoryLayout.ROW_LAYOUT);
    }

    public PagedDBufferValue(final int rows,  final int cols, final List<double[]> buffers, MemoryLayout layout) {
        super(rows * cols * Double.BYTES, false);

        this.rows           = rows;
        this.cols           = cols;
        this.layout         = Preconditions.checkNotNull(layout);
        this.segmentSize    = MemoryManager.DEFAULT_MEMORY_SEGMENT_SIZE;//MemoryManager.getMemoryManager().getSegmentSize();
        this.buffers        = Preconditions.checkNotNull(buffers);

        //LOG.info("Created PagedDoubleBufferValue: rows = " + rows + ", cols = " + cols
        //        + ", partitionSize = " + getPartitionSize() + ", buffers = " + buffers.size());
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
    public DMatrix.RowIterator rowIterator() { return new RowIterator(this); }

    @Override
    public double[] toArray() { throw new UnsupportedOperationException(); }

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

    // ---------------------------------------------------

    /*private final class SegmentSwapManager {

        private final BlockingQueue<Pair<Integer,double[]>> writeQueue;

        private final BlockingQueue<Pair<Integer,double[]>> readQueue;

        private volatile boolean isRunning = true;

        public SegmentSwapManager() {

            this.writeQueue = new LinkedBlockingQueue<>();

            this.readQueue  = new LinkedBlockingQueue<>();

            final Runnable writerThread = () -> {

                while (isRunning) {

                    try {

                        final Pair<Integer,double[]> segment = writeQueue.take();

                        final File segFile = new File("tmp-" + key.internalUID + "-" + segment.getLeft());

                        final byte[] seg = UnsafeOp.primitiveArrayTypeCast(segment.getRight(), double[].class, byte[].class);

                        final FileOutputStream os = new FileOutputStream(segFile);

                        os.write(seg);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            final Runnable readerThread = () -> {

                while (isRunning) {

                    try {

                        final Pair<Integer,double[]> segment = readQueue.take();

                        final File segFile = new File("tmp-" + key.internalUID + "-" + segment.getLeft());

                        final FileInputStream is = new FileInputStream(segFile);

                        final byte[] seg = UnsafeOp.primitiveArrayTypeCast(segment.getRight(), double[].class, byte[].class);

                        is.read(seg);

                        UnsafeOp.primitiveArrayTypeCast(seg, byte[].class, double[].class);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        }
    }*/
}
