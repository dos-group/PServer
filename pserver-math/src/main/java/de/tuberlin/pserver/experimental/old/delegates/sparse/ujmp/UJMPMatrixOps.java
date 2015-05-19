package de.tuberlin.pserver.experimental.old.delegates.sparse.ujmp;

import de.tuberlin.pserver.experimental.old.SMatrix;
import de.tuberlin.pserver.experimental.old.SVector;
import de.tuberlin.pserver.experimental.old.delegates.LibraryMatrixOps;
import org.ujmp.core.matrix.SparseMatrix;

public final class UJMPMatrixOps implements LibraryMatrixOps<SMatrix, SVector> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public SMatrix add(final SMatrix B, final SMatrix A) {
        final SparseMatrix b = convertSMatrixToUJMPSparseMatrix(B);
        final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        a.plus(b);
        return A;
    }

    @Override
    public SMatrix sub(final SMatrix B, final SMatrix A) {
        final SparseMatrix b = convertSMatrixToUJMPSparseMatrix(B);
        final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        a.minus(b);
        return A;
    }

    @Override
    public SVector mul(final SMatrix A, final SVector X, final SVector Y) {
        //final SparseMatrix x = convertSVectorToUJMPSparseMatrix(X);
        //final SparseMatrix y = convertSVectorToUJMPSparseMatrix(Y);
        //final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        return null;
    }

    @Override
    public SMatrix scale(final double alpha, final SMatrix A) {
        final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        a.mtimes(alpha);
        return A;
    }

    @Override
    public SMatrix transpose(final SMatrix A) {
        final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        a.transpose();
        return A;
    }

    @Override
    public SMatrix transpose(final SMatrix B, final SMatrix A) {
        final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        final SparseMatrix b = convertSMatrixToUJMPSparseMatrix(B);
        b.setAsMatrix(a, A.numRows(), A.numCols());
        b.transpose();
        return B;
    }

    @Override
    public boolean invert(final SMatrix A) {
        final SparseMatrix a = convertSMatrixToUJMPSparseMatrix(A);
        a.inv();
        return true; // HMMMMM....
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    private static SparseMatrix convertSMatrixToUJMPSparseMatrix(final SMatrix matrix) {
        return (SparseMatrix)matrix.getInternalMatrix();
    }

    private static SparseMatrix convertSVectorToUJMPSparseMatrix(final SVector vector) {
        return (SparseMatrix)vector.getInternalVector();
    }
}
