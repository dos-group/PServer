package de.tuberlin.pserver.math.matrix.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.dense.DVector;

import java.io.Serializable;
import java.util.Random;

public class Dense64Matrix extends AbstractMatrix implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //private static final Logger LOG = LoggerFactory.getLogger(DMatrix.class);

    private static final LibraryMatrixOps<Matrix, Vector> matrixOpDelegate =
            MathLibFactory.delegateDMatrixOpsTo(MathLibFactory.DMathLibrary.EJML_LIBRARY);

    private double[] data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // Copy Constructor.
    public Dense64Matrix(final Vector m) {
        super(m.layout() == Layout.COLUMN_LAYOUT ?
                1 : m.length(),
              m.layout() == Layout.COLUMN_LAYOUT ?
                m.length() : 1,
              m.layout() == Layout.COLUMN_LAYOUT ?
                Layout.COLUMN_LAYOUT : Layout.ROW_LAYOUT);

        final double[] md = m.toArray();
        this.data = new double[md.length];
        System.arraycopy(md, 0, this.data, 0, md.length);
    }

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
    public void set(long row, long col, double value) { data[Utils.getPos(row, col, this)] = value; }

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
    public Vector mul(Vector b, Vector c) {
        return super.mul(b, c);
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
    public Vector rowAsVector() {
        return rowAsVector(0, 0, cols());
    }

    @Override
    public Vector rowAsVector(final long row) {
        return rowAsVector(row, 0, cols());
    }

    @Override
    public Vector rowAsVector(final long row, final long from, final long to) { // TODO: Optimize with respect to the layout with array copy.
        Vector r = new DVector(to - from);
        for (long i = from; i < to; ++i)
            r.set(i, data[Utils.getPos(row, i, this)]);
        return r;
    }

    @Override
    public Vector colAsVector() {
        return colAsVector(0, 0, rows);
    }

    @Override
    public Vector colAsVector(final long col) {
        return colAsVector(col, 0, rows);
    }

    @Override
    public Vector colAsVector(final long col, final long from, final long to) {
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
        return new DVector(result.length, result);
    }

    @Override
    public Matrix assignRow(final long row, final Vector v) {
        Preconditions.checkNotNull(cols() == v.length());
        for (int i = 0; i < v.length(); ++i)
            data[Utils.getPos(row, i, this)] = v.get(i);
        return this;
    }

    @Override
    public Matrix assignColumn(final long col, final Vector v) {
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

        private int globalRowIndex;

        private final int end;

        private final int start;

        private final int numRows;

        private int currentRowIndex;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final Dense64Matrix v) { this(v, 0, (int)Preconditions.checkNotNull(v).rows() - 1); }
        public RowIterator(final Dense64Matrix v, final int startRow, final int endRow) {
            this.self = v;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.rows());
//            Preconditions.checkArgument(endRow > startRow && endRow < self.rows());
            this.start = startRow * (int)self.cols;
            this.end = endRow * (int)self.cols;
            this.globalRowIndex = this.start - (int)-self.cols;
            this.numRows = endRow - startRow;
            this.rand = new Random();
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNext() { return globalRowIndex < end - self.cols/*|| globalRowIndex < self.rows * self.cols*/; }

        @Override
        public void next() { globalRowIndex += self.cols; currentRowIndex = globalRowIndex; }

        @Override
        public void nextRandom() {
            globalRowIndex += self.cols;
            currentRowIndex = start +  (rand.nextInt(numRows) * (int)self.cols);
        }

        @Override
        public double value(final long col) { return self.data[(int)(currentRowIndex + col)]; }

        @Override
        public Vector asVector() { return asVector(0, (int) self.cols); }

        @Override
        public Vector asVector(int from, int size) {
            final double v[] = new double[size];
            System.arraycopy(self.data, currentRowIndex + from, v, 0, size);
            return new DVector(size, v);
        }

        @Override
        public void reset() { globalRowIndex = start - (int)self.cols; }

        @Override
        public long rows() { return self.rows; }

        @Override
        public long cols() { return self.cols; }

        @Override
        public int rowNum() { return currentRowIndex / (int)self.cols; }
    }
}
