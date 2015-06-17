package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.CompColMatrix;
import no.uib.cipr.matrix.sparse.CompRowMatrix;

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

    public SMatrix(no.uib.cipr.matrix.Matrix mat, MemoryLayout layout) {
        super(mat.numRows(), mat.numColumns(), layout);
        data = mat;
    }

    /**
     * Builds a MTJ CompRowMatrix from a Pserver DMatrix by iterating over the rows of @param mat and constructing the
     * int[][] nz structure. In another iteration the non-zero elements are set.
     * @param mat The Pserver DMatrix
     * @return An SMatrix instance containing the constructed MTJ CompRowMatrix
     */
    public static SMatrix toCompRowMatrix(DMatrix mat) {
        int[][] nz = new int[(int)mat.numRows()][];
        double[] data = mat.toArray();
        for(long i = 0; i < mat.numRows(); i++) {
            int[] buffer = new int[(int)mat.numCols()];
            int bufLength = 0;
            for(long j = 0; j < mat.numCols(); j++) {
                if(mat.get(i, j) != 0.0) {
                    buffer[bufLength] = (int)j;
                    bufLength++;
                }
            }
            nz[(int)i] = java.util.Arrays.copyOf(buffer, bufLength);
        }
        no.uib.cipr.matrix.AbstractMatrix mtjMat = new CompRowMatrix((int)mat.numRows(), (int)mat.numCols(), nz);
        for(long i = 0; i < mat.numRows(); i++) {
            for(long j = 0; j < mat.numCols(); j++) {
                if(mat.get(i, j) != 0.0) {
                    mtjMat.set((int)i, (int)j, mat.get(i, j));
                }
            }
        }
        return null;
        //return new MTJMatrix(mat.numRows(), mat.numCols(), mtjMat, MemoryLayout.COMPRESSED_ROW);
    }

    @Override
    public Matrix assign(double v) {
        double[] data = new double[Utils.toInt(rows*cols)];
        return new DMatrix(rows, cols, data);
    }

    @Override
    public Vector viewRow(long row) {
        return null;
    }

    @Override
    public Vector viewColumn(long col) {
        return null;
    }

    @Override
    public Matrix assignRow(long row, Vector v) {
        return null;
    }

    @Override
    public Matrix assignColumn(long col, Vector v) {
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

    private no.uib.cipr.matrix.Matrix getNewInstance(int[][] nz) {
        no.uib.cipr.matrix.AbstractMatrix result;
        switch(layout) {
            case ROW_LAYOUT:
                result = new CompRowMatrix(Utils.toInt(rows), Utils.toInt(cols), nz);
                break;
            case COLUMN_LAYOUT:
                result = new CompColMatrix(Utils.toInt(rows), Utils.toInt(cols), nz);
                break;
            default:
                throw new IllegalArgumentException("Unknown MemoryLayout: " + layout.toString());
        }
        return result;
    }

    @Override
    public void setArray(double[] data) {
        Preconditions.checkArgument(data.length == rows * cols, String.format("Wrong length of data array. Excepted: rows * cols = %d * %d = %d. Actual: %d", rows, cols, rows*cols, data.length));
        this.data = null;//getNewInstance(buildNz(data));
        for(long i = 0; i < rows; i++) {
            for(long j = 0; j < cols; j++) {
                double val = data[Utils.getPos(i, j, layout, rows, cols)];
                if(val != 0.0) {
                    this.data.set(Utils.toInt(i), Utils.toInt(j), val);
                }
            }
        }
    }

    @Override
    public Matrix.RowIterator randomRowIterator() {
        return null;
    }

    @Override
    public Matrix.RowIterator randomRowIterator(int startRow, int endRow) {
        return null;
    }

    @Override
    public Matrix axpy(double alpha, Matrix B) {
        return null;
    }

    public no.uib.cipr.matrix.Matrix getContainer() {
        return data;
    }


    class Entry {
        // fields
        private final long row, col;
        private final double value;
        // constructor
        public Entry(long row, long col, double value) {
            this.row = row;
            this.col = col;
            this.value = value;
        }
        // getter
        public long getRow() { return row; }
        public long getCol() { return col; }
        public double getValue() { return value; }
    }

}
