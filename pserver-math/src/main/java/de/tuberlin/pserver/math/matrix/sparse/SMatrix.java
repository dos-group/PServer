package de.tuberlin.pserver.math.matrix.sparse;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.delegates.sparse.mtj.MTJUtils;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.math.vector.sparse.SVector;
import de.tuberlin.pserver.math.vector.Vector;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.CompColMatrix;
import no.uib.cipr.matrix.sparse.CompRowMatrix;
import no.uib.cipr.matrix.sparse.FlexCompColMatrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;

import java.util.Iterator;

public class SMatrix extends AbstractMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected no.uib.cipr.matrix.Matrix data;

    private static final LibraryMatrixOps<Matrix, Vector> matrixOpDelegate =
            MathLibFactory.delegateSMatrixOpsTo(MathLibFactory.SMathLibrary.MTJ_LIBRARY);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SMatrix() {
        super(0, 0, Matrix.Layout.ROW_LAYOUT);
    }

    public SMatrix(no.uib.cipr.matrix.Matrix mat, Layout layout) {
        super(mat.numRows(), mat.numColumns(), layout);
        data = mat;
    }

    public SMatrix(final long rows, final long cols, final Layout layout) {
        super(rows, cols, layout);
        switch(layout) {
            case ROW_LAYOUT:
                data = new FlexCompColMatrix(Utils.toInt(numRows()), Utils.toInt(numCols()));
                break;
            case COLUMN_LAYOUT:
                data = new FlexCompRowMatrix(Utils.toInt(numRows()), Utils.toInt(numCols()));
                break;
            default:
                throw new IllegalArgumentException("Unknown Memory Layout " + getLayout().toString());
        }
    }

    public static SMatrix fromDMatrix(DMatrix mat) {
        return fromDMatrix(mat, mat.getLayout(), false);
    }

    public static SMatrix fromDMatrix(DMatrix mat, Layout targetLayout) {
        return fromDMatrix(mat, targetLayout, false);
    }

    public static SMatrix fromDMatrix(DMatrix mat, boolean mutable) {
        return fromDMatrix(mat, mat.getLayout(), mutable);
    }

    public static SMatrix fromDMatrix(DMatrix mat, Layout targetLayout, boolean mutable) {
        no.uib.cipr.matrix.Matrix data = null;
        if(!mutable) { // create Comp[Row|Col]Matrix
            switch(targetLayout) {
                case ROW_LAYOUT:
                    data = new CompRowMatrix(Utils.toInt(mat.numRows()), Utils.toInt(mat.numCols()), MTJUtils.buildRowBasedNz(mat));
                    setNonZeroElements(mat, data);
                    break;
                case COLUMN_LAYOUT:
                    data = new CompColMatrix(Utils.toInt(mat.numRows()), Utils.toInt(mat.numCols()), MTJUtils.buildColBasedNz(mat));
                    setNonZeroElements(mat, data);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Memory Layout " + mat.getLayout().toString());
            }
        }
        else { // create FlexComp[Row|Col]Matrix
            switch(targetLayout) {
                case ROW_LAYOUT:
                    data = new FlexCompColMatrix(Utils.toInt(mat.numRows()), Utils.toInt(mat.numCols()));
                    setNonZeroElements(mat, data);
                    break;
                case COLUMN_LAYOUT:
                    data = new FlexCompRowMatrix(Utils.toInt(mat.numRows()), Utils.toInt(mat.numCols()));
                    setNonZeroElements(mat, data);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Memory Layout " + mat.getLayout().toString());
            }
        }
        Preconditions.checkState(data != null, "Can not convert DMatrix to SMatrix");
        return new SMatrix(data, targetLayout);
    }

    public static void setNonZeroElements(Matrix mat, no.uib.cipr.matrix.Matrix data) {
        for(long i = 0; i < mat.numRows(); i++) {
            for(long j = 0; j < mat.numCols(); j++) {
                if( ! Utils.closeTo(mat.get(i, j), 0.0)) {
                    data.set(Utils.toInt(i), Utils.toInt(j), mat.get(i, j));
                }
            }
        }
    }

    @Override
    public Matrix assign(double v) {
        double[] data = new double[Utils.toInt(rows*cols)];
        return new DMatrix(rows, cols, data);
    }

    @Override
    public Matrix assignRow(long row, Vector v) {
        return null;
    }

    @Override
    public Matrix assignColumn(long col, Vector v) {
        return null;
    }

    @Override
    public Matrix copy() {
        return null;
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Matrix add(final Matrix B) { return matrixOpDelegate.add(B, this); }

    @Override public Matrix sub(final Matrix B) { return matrixOpDelegate.sub(B, this); }

    @Override public Matrix mul(final Matrix B) { return matrixOpDelegate.mul(this, B); }

    @Override public Vector mul(final Vector v) { return matrixOpDelegate.mul(this, v); }

    @Override public void mul(final Vector x, final Vector y) { matrixOpDelegate.mul(this, x, y); }

    @Override public Matrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    @Override public Matrix transpose() { return matrixOpDelegate.transpose(this); }

    @Override public void transpose(final Matrix B) { matrixOpDelegate.transpose(this, B); }

    @Override public boolean invert() { return matrixOpDelegate.invert(this); }

    @Override
    public Matrix assign(Matrix v) {
        return null;
    }

    @Override
    public double get(long row, long col) { return data.get(Utils.toInt(row), Utils.toInt(col)); }

    @Override
    public void set(long row, long col, double value) { data.set(Utils.toInt(row), Utils.toInt(col), value); }

    @Override
    public double atomicGet(long row, long col) {
        synchronized (data) {
            return data.get(Utils.toInt(row), Utils.toInt(col));
        }
    }

    @Override
    public void atomicSet(long row, long col, double value) {
        synchronized (data) {
            data.set(Utils.toInt(row), Utils.toInt(col), value);
        }
    }

    @Override
    public double[] toArray() {
        double[] result = new double[Utils.toInt(rows*cols)];
        Iterator<MatrixEntry> iter = data.iterator();
        while(iter.hasNext()) {
            MatrixEntry entry = iter.next();
            result[Utils.getPos(entry.row(), entry.column(), layout, rows, cols)] = entry.get();
        }
        return result;
    }

    public double[] toArray(Layout targetLayout) {
        double[] result = new double[Utils.toInt(rows*cols)];
        Iterator<MatrixEntry> iter = data.iterator();
        while(iter.hasNext()) {
            MatrixEntry entry = iter.next();
            result[Utils.getPos(entry.row(), entry.column(), targetLayout, rows, cols)] = entry.get();
        }
        return result;
    }

    @Override
    public void setArray(double[] data) {
        Preconditions.checkArgument(data.length == rows * cols, String.format("Wrong length of data array. Excepted: rows * cols = %d * %d = %d. Actual: %d", rows, cols, rows*cols, data.length));
        this.data = SMatrix.fromDMatrix(new DMatrix(rows, cols, data, layout), layout, true).getContainer();
    }

    @Override
    public Matrix axpy(double alpha, Matrix B) {
        return null;
    }

    public no.uib.cipr.matrix.Matrix getContainer() {
        return data;
    }

    @Override
    public RowIterator rowIterator() {
        return rowIterator(0, Utils.toInt(rows - 1));
    }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) {
        return new SparseRowIterator(this, startRow, endRow);
    }


    @Override
    public Vector rowAsVector() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector rowAsVector(long row) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector rowAsVector(long row, long from, long to) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector colAsVector() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector colAsVector(long col) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Vector colAsVector(long col, long from, long to) {
        throw new UnsupportedOperationException("");
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class SparseRowIterator extends AbstractRowIterator {

        public SparseRowIterator(AbstractMatrix mat, int startRow, int endRow) {
            super(mat, startRow, endRow);
        }

        @Override
        public void nextRandomRow() {
            throw new UnsupportedOperationException("");
        }

        @Override
        public Vector getAsVector() {
            return getAsVector(0, Utils.toInt(target.numCols()), new SVector(target.numCols(), Vector.Layout.COLUMN_LAYOUT));
        }

        @Override
        public Vector getAsVector(int from, int size) {
            return getAsVector(from, size, new SVector(size, Vector.Layout.COLUMN_LAYOUT));
        }
    }
}
