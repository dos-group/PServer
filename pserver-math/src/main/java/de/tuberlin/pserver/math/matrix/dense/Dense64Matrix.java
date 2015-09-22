package de.tuberlin.pserver.math.matrix.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;

import javax.rmi.CORBA.Util;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

public class Dense64Matrix extends AbstractMatrix implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //private static final Logger LOG = LoggerFactory.getLogger(DMatrix.class);

    private static final LibraryMatrixOps<Matrix> matrixOpDelegate =
            MathLibFactory.delegateDMatrixOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    private double[] data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // Copy Constructor.
    public Dense64Matrix(final Dense64Matrix m) {
        super(m.rows, m.cols, m.layout);
        this.data = new double[m.data.length];
        System.arraycopy(m.data, 0, this.data, 0, m.data.length);
    }

    public Dense64Matrix(final long rows, final long cols) { this(rows, cols, null, Layout.ROW_LAYOUT); }
    public Dense64Matrix(final long rows, final long cols, final double[] data) { this(rows, cols, data, Layout.ROW_LAYOUT); }
    public Dense64Matrix(final long rows, final long cols, final double[] data, final Layout layout) {
        super(rows, cols, layout);
        this.data = (data == null) ? new double[(int)(rows * cols)] : Preconditions.checkNotNull(data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public double get(final long index) { return data[(int)index]; }

    @Override
    public double get(final long row, final long col) { return data[Utils.getPos(row, col, this)]; }

    @Override
    public void set(long row, long col, double value) {
        try {
            data[Utils.getPos(row, col, this)] = value;
        }
        catch(ArrayIndexOutOfBoundsException e) {
            if(row < rows && col < cols) {
                throw new IllegalStateException(String.format("Attempt to set a valid position (%d, %d) in matrix of shape (%d, %d) yielded ArrayIndexOutOfBounds: %d", row, col, rows, cols, Utils.getPos(row, col, this)), e);
            }
            throw new IllegalStateException(String.format("Attempt to set a invalid position (%d, %d) in matrix of shape (%d, %d) yielded ArrayIndexOutOfBounds: %d", row, col, rows, cols, Utils.getPos(row, col, this)), e);
        }
    }

    @Override
    public double[] toArray() {
        return data;
    }

    @Override
    public void setArray(final double[] data) { Preconditions.checkState(data.length == rows * cols); this.data = data; }

    @Override
    public RowIterator rowIterator() { return new RowIterator(this); }

    @Override
    public RowIterator rowIterator(final int startRow, final int endRow) { return new RowIterator(this, startRow, endRow); }

    @Override
    protected Matrix newInstance(long rows, long cols) {
        return new Dense64Matrix(rows, cols);
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override
    public Matrix add(Matrix B, Matrix C) {
        Utils.checkShapeEqual(this, B, C);
        return matrixOpDelegate.add(this, B, C);
    }

    @Override
    public Matrix sub(Matrix B, Matrix C) {
        Utils.checkShapeEqual(this, B, C);
        return matrixOpDelegate.sub(this, B, C);
    }

    @Override
    public Matrix mul(Matrix B, Matrix C) {
        Utils.checkShapeMatrixMatrixMult(this, B, C);
        return matrixOpDelegate.mul(this, B, C);
    }

    @Override
    public Matrix scale(double a, Matrix B) {
        return super.scale(a, B);
    }

    @Override
    public Matrix transpose(Matrix B) {
        return super.transpose(B);
    }

    @Override
    public Matrix invert(Matrix B) {
        return super.invert(B);
    }

    @Override
    public Matrix assign(final Matrix v) {
        Preconditions.checkState(v.rows() * v.cols() == rows * cols);
        System.arraycopy(v.toArray(), 0, data, 0, (int)(rows * cols));
        return this;
    }

    @Override
    public Matrix assign(final double v) {
        for (int i = 0; i < rows * cols; ++i)
            data[i] = v;
        return this;
    }

    @Override
    public Matrix getRow(final long row) {
        return getRow(row, 0, cols());
    }

    @Override
    public Matrix getRow(final long row, final long from, final long to) {
        // TODO: Optimize with respect to the layout with array copy.
        Matrix r = new Dense64Matrix(1, to - from);
        for (long i = from; i < to; ++i)
            r.set(0, i, data[Utils.getPos(row, i, this)]);
        return r;
    }

    @Override
    public Matrix getCol(final long col) {
        return getCol(col, 0, rows);
    }

    @Override
    public Matrix getCol(final long col, final long from, final long to) {
        double[] result = new double[(int)(to - from)];
        if(layout == Layout.COLUMN_LAYOUT) {
            System.arraycopy(data, (int)(col * rows + from), result, 0, result.length);
        }
        else {
            for (int i = 0; i < result.length; i++) {
                int row = (int)from + i;
                result[i] = data[(int)(row * cols + col)];
            }
        }
        return new Dense64Matrix(result.length, 1, result, Layout.COLUMN_LAYOUT);
    }

    @Override
    public Matrix assignRow(final long row, final Matrix v) {
        Preconditions.checkNotNull(cols == v.cols());
        for (int i = 0; i < cols; ++i)
            data[Utils.getPos(row, i, this)] = v.get(i);
        return this;
    }

    @Override
    public Matrix assignColumn(final long col, final Matrix v) {
        double[] vData = v.toArray();
        Preconditions.checkArgument(rows == vData.length);
        if(layout == Layout.COLUMN_LAYOUT) {
            System.arraycopy(v.toArray(), 0, data, (int)(col * rows), vData.length);
        }
        else {
            for (int row = 0; row < rows; row++) {
                data[(int)(row * cols + col)] = vData[row];
            }
        }
        return this;
    }

    @Override
    public Matrix copy() {
        return new Dense64Matrix(this);
    }

    @Override
    public Matrix subMatrix(long row, long col, long rows, long cols) {
        if (layout == Layout.ROW_LAYOUT) {
            final int length = (int)(rows * cols);
            final double[] subData = new double[length];
            System.arraycopy(data, (int)(row * cols + col), subData, 0, length);
            return new Dense64Matrix(rows, cols, subData, layout);
        } else
            throw new UnsupportedOperationException();
    }

    @Override
    public Matrix assign(final long row, final long col, final Matrix m) {
        if (layout == Layout.ROW_LAYOUT && m.layout() == Layout.ROW_LAYOUT) {
            if (cols == m.cols())
                System.arraycopy(m.toArray(), 0, data, (int)(row * cols + col), m.toArray().length);
            else
                throw new IllegalStateException();
            return this;
        } else
            throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "Dense64Matrix{" +
                "data=" + Arrays.toString(data) +
                '}';
    }

    // ---------------------------------------------------

    //@Override public Matrix add(final Matrix B) { return matrixOpDelegate.add(B, this); }

    //@Override public Matrix sub(final Matrix B) { return matrixOpDelegate.sub(B, this); }

    //@Override public Matrix mul(final Matrix B) { return matrixOpDelegate.mul(this, B); }

    //@Override public Vector mul(final Vector v) { return matrixOpDelegate.mul(this, v); }

    //@Override public void mul(final Vector x, final Vector y) { matrixOpDelegate.mul(this, x, y); }

    //@Override public Matrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    //@Override public Matrix transpose() { return matrixOpDelegate.transpose(this); }

    //@Override public void transpose(final Matrix B) { matrixOpDelegate.transpose(this, B); }

    //@Override public boolean invert() { return matrixOpDelegate.invert(this); }

    // ---------------------------------------------------

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class RowIterator implements Matrix.RowIterator {

        private Dense64Matrix self;

        private final int end;

        private final int start;

        private int currentRow;

        private final int rowsToFetch;

        private int rowsFetched;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final Dense64Matrix v) { this(v, 0, (int)Preconditions.checkNotNull(v).rows()); }
        public RowIterator(final Dense64Matrix v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.rows());
            Preconditions.checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand = new Random();
            reset();
            System.out.println("inner Dense64Matrix.rowIterator! start: "+start+"; end: "+end+"; rowsToFetch: "+ rowsToFetch +";");
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNext() {
            return rowsFetched < rowsToFetch;
        }

        @Override
        public void next() {
            // the generic case is just currentRow++, but the reset method only sets rowsFetched = 0
            if(rowsFetched == 0) {
                currentRow = 0;
            }
            else {
                currentRow++;
            }
            rowsFetched++;
            // can overflow if nextRandom and next is called alternatingly
            if(currentRow >= end) {
                currentRow = start;
            }
        }

        @Override
        public void nextRandom() {
            rowsFetched++;
            currentRow = start + rand.nextInt(end);
        }

        @Override
        public double value(final long col) {
            return self.data[Utils.getPos(currentRow, col, self)];
        }

        @Override
        public Matrix get() {
            return get(0, (int) self.cols);
        }

        @Override
        public Matrix get(int from, int size) {
            final double v[] = new double[size];
            if(self.layout == Layout.ROW_LAYOUT) {
                try {
                    System.arraycopy(self.data, Utils.getPos(currentRow, from, self), v, 0, size);
                }
                catch(ArrayIndexOutOfBoundsException e) {
                    System.out.println("failed copy from: " + Utils.getPos(currentRow, from, self) + "; currentRow: " + currentRow + "; from: " + from + ";  length: " + size + "; array.length: " + self.data.length);
                    throw e;
                }

            }
            else {
                for (int i = from; i < size; i++) {
                    v[i-from] = self.data[Utils.getPos(currentRow, i, self)];
                }
            }
            return new Dense64Matrix(1, size, v);
        }

        @Override
        public void reset() {
            rowsFetched = 0;
        }

        @Override
        public long rows() { return self.rows; }

        @Override
        public long cols() { return self.cols; }

        @Override
        public int rowNum() { return currentRow; }
    }
}
