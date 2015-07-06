package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.stuff.*;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractMatrix implements Matrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected Object owner;

    protected final long rows;

    protected final long cols;

    protected Lock lock;

    protected final Layout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractMatrix(long rows, long cols, Layout layout) {
        this.rows = rows;
        this.cols = cols;
        this.layout = Preconditions.checkNotNull(layout);
        Preconditions.checkArgument(java.util.Arrays.asList(Layout.values()).contains(layout), "Unknown MemoryLayout: " + layout.toString());
        this.lock = new ReentrantLock();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override  public long sizeOf() { return rows * cols * Double.BYTES; }

    @Override  public void setOwner(Object owner) { this.owner = owner; }

    @Override  public Object getOwner() { return owner; }

    @Override  public long numRows() { return rows; }

    @Override  public long numCols() { return cols; }

    @Override  public Layout getLayout() { return layout; }

    @Override  public abstract RowIterator rowIterator();

    @Override  public abstract RowIterator rowIterator(int startRow, int endRow);

    @Override
    public double aggregate(DoubleDoubleFunction combiner, DoubleFunction mapper) {
        return aggregateRows(new VectorFunction() {
            @Override
            public double apply(Vector v) {
                return v.aggregate(combiner, mapper);
            }
        }).aggregate(combiner, Functions.IDENTITY);
    }

    @Override
    public Vector aggregateRows(final VectorFunction f) {
        Vector r = new DVector(numRows());
        long n = numRows();
        for (int row = 0; row < n; row++) {
            r.set(row, f.apply(rowAsVector(row)));
        }
        return r;
    }

    @Override
    public void lock() { lock.lock(); }

    @Override
    public void unlock() { lock.unlock(); }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static abstract class AbstractRowIterator implements Matrix.RowIterator {

        protected AbstractMatrix target;

        protected int currentRow;

        protected final int startRow;

        protected final int endRow;

        protected int numFetched;

        protected Random rand = new Random();

        public AbstractRowIterator(final AbstractMatrix mat) { this(mat, 0, Utils.toInt(Preconditions.checkNotNull(mat).numRows()) - 1); }
        public AbstractRowIterator(final AbstractMatrix mat, final int startRow, final int endRow) {
            this.target = mat;
            Preconditions.checkArgument(startRow >= 0 && startRow < target.numRows());
            Preconditions.checkArgument(endRow > startRow && endRow < target.numRows());
            this.startRow = startRow;
            this.endRow = endRow;
            reset();
        }

        @Override
        public boolean hasNextRow() {
            return numFetched < (endRow - startRow + 1);
        }

        @Override
        public void nextRow() {
            numFetched++;
            currentRow++;
        }

        @Override
        public void nextRandomRow() {
            numFetched++;
            currentRow = startRow + rand.nextInt(endRow+1);
        }

        @Override
        public double getValueOfColumn(final int col) { return target.get(currentRow, col); }

        protected Vector getAsVector(int from, int size, Vector result) {
            Preconditions.checkArgument(from + size <= target.numCols());
            Preconditions.checkArgument(result.length() == size);
            for(int i = from; i - from < size; i++) {
                result.set(i, target.get(currentRow, i));
            }
            return result;
        }

        @Override
        public abstract Vector getAsVector();

        @Override
        public abstract Vector getAsVector(int from, int size);

        @Override
        public void reset() {
            currentRow = startRow - 1;
            numFetched = 0;
        }

        @Override
        public long numRows() { return target.rows; }

        @Override
        public long numCols() { return target.cols; }

    }
}
