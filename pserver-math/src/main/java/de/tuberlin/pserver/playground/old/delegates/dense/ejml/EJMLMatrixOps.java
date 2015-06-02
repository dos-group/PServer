package de.tuberlin.pserver.playground.old.delegates.dense.ejml;

import de.tuberlin.pserver.playground.old.DMatrix;
import de.tuberlin.pserver.playground.old.DVector;
import de.tuberlin.pserver.playground.old.delegates.LibraryMatrixOps;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

public final class EJMLMatrixOps implements LibraryMatrixOps<DMatrix, DVector> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public DMatrix add(final DMatrix B, final DMatrix A) {
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.add(a, b, a);
        return A;
    }

    @Override
    public DMatrix sub(final DMatrix B, final DMatrix A) {
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.sub(a, b, a);
        return A;
    }

    @Override
    public DVector mul(final DMatrix A, final DVector X, final DVector Y) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F x = EJMLVectorOps.convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = EJMLVectorOps.convertDVectorToDenseVector64F(Y);
        CommonOps.mult(a, x, y);
        return Y;
    }

    @Override
    public DMatrix scale(final double alpha, final DMatrix A) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.scale(alpha, a);
        return A;
    }

    @Override
    public DMatrix transpose(final DMatrix A) {
        CommonOps.transpose(convertDMatrixToDenseMatrix64F(A));
        return A;
    }

    @Override
    public DMatrix transpose(final DMatrix B, final DMatrix A) {
        CommonOps.transpose(convertDMatrixToDenseMatrix64F(A), convertDMatrixToDenseMatrix64F(B));
        return B;
    }

    @Override
    public boolean invert(final DMatrix A) {
        return CommonOps.invert(convertDMatrixToDenseMatrix64F(A));
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    private static DenseMatrix64F convertDMatrixToDenseMatrix64F(final DMatrix matrix) {
        return new DenseMatrix64F((int)matrix.numRows(), (int)matrix.numCols(), true, matrix.toArray());
    }
}
