package de.tuberlin.pserver.math.delegates.dense.ejml;

import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.matrix.Matrix;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

public final class EJMLMatrixOps implements LibraryMatrixOps<Matrix> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Matrix add(final Matrix A, final Matrix B, final Matrix C) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F c = convertDMatrixToDenseMatrix64F(C);
        CommonOps.add(a, b, c);
        return C;
    }

    @Override
    public Matrix sub(final Matrix A, final Matrix B, final Matrix C) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F c = convertDMatrixToDenseMatrix64F(C);
        CommonOps.sub(a, b, c);
        return C;
    }

    @Override
    public Matrix mul(final Matrix A, final Matrix B, Matrix C) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F c = convertDMatrixToDenseMatrix64F(C);
        CommonOps.mult(a, b, c);
        return C;
    }

    @Override
    public Matrix scale(final Matrix A, final double alpha, final Matrix B) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        CommonOps.scale(alpha, a, b);
        return B;
    }

    @Override
    public Matrix transpose(final Matrix A, final Matrix B) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        CommonOps.transpose(a,b);
        return B;
    }

    @Override
    public boolean invert(final Matrix A, final Matrix B) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        return CommonOps.invert(a, b);
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    private static DenseMatrix64F convertDMatrixToDenseMatrix64F(final Matrix matrix) {
        return DenseMatrix64F.wrap((int)matrix.rows(), (int)matrix.cols(), matrix.toArray());
    }
}
