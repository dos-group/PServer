package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.delegates.DelegationMatrix;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.delegates.MathLibFactory;
import de.tuberlin.pserver.math.delegates.sparse.mtj.MTJMatrix;
import no.uib.cipr.matrix.sparse.CompRowMatrix;

public abstract class SMatrix<T> extends DelegationMatrix<T> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum MemoryLayout {

        COMPRESSED_ROW,

        COMPRESSED_COL
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final MemoryLayout layout;

    private static final LibraryMatrixOps<SMatrix, SVector> matrixOpDelegate =
            MathLibFactory.delegateSMatrixOpsTo(MathLibFactory.SMathLibrary.MTJ_LIBRARY);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected SMatrix(final long rows, final long cols, T target, MemoryLayout layout) {
        super(rows, cols, target);
        this.layout = Preconditions.checkNotNull(layout);
        Preconditions.checkArgument(java.util.Arrays.asList(MemoryLayout.values()).contains(layout), "Unknown MemoryLayout: " + layout.toString());
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
        return new MTJMatrix(mat.numRows(), mat.numCols(), mtjMat, MemoryLayout.COMPRESSED_ROW);
    }

    @Override
    public Matrix assign(double v) {
        double[] data = new double[Utils.toInt(rows*cols)];
        return new DMatrix(rows, cols, data);
    }

    // ---------------------------------------------------
    // Matrix Operation Delegates.
    // ---------------------------------------------------

    @Override public Matrix add(final Matrix B) { return matrixOpDelegate.add((SMatrix)B, this); }

    @Override public Matrix sub(final Matrix B) { return matrixOpDelegate.sub((SMatrix)B, this); }

    @Override public Matrix mul(final Matrix B) { return matrixOpDelegate.mul(this, (SMatrix)B); }

    @Override public Vector mul(final Vector v) { return matrixOpDelegate.mul(this, (SVector)v); }

    @Override public void mul(final Vector x, final Vector y) { matrixOpDelegate.mul(this, (SVector)x, (SVector)y); }

    @Override public Matrix scale(final double alpha) { return matrixOpDelegate.scale(alpha, this); }

    @Override public Matrix transpose() { return matrixOpDelegate.transpose(this); }

    @Override public void transpose(final Matrix B) { matrixOpDelegate.transpose(this, (SMatrix)B); }

    @Override public boolean invert() { return matrixOpDelegate.invert(this); }



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
