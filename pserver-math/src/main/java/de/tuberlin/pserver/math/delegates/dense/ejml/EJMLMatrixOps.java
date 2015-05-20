package de.tuberlin.pserver.math.delegates.dense.ejml;

import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.Vector;
import org.ejml.alg.dense.mult.MatrixVectorMult;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

public final class EJMLMatrixOps implements LibraryMatrixOps<Matrix, Vector> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Matrix add(final Matrix B, final Matrix A) {
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.add(a, b, a);
        return A;
    }

    @Override
    public Matrix sub(final Matrix B, final Matrix A) {
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.sub(a, b, a);
        return A;
    }

    @Override
    public Vector mul(final Matrix A, final Vector X) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = EJMLVectorOps.convertDVectorToDenseVector64F(X);
        final DenseMatrix64F c = new DenseMatrix64F(a.getNumRows(), b.getNumCols(), true, b.getData());
        MatrixVectorMult.mult(a, b, c);
        return X;
    }

    @Override
    public Matrix mul(final Matrix A, final Matrix B) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);;
        CommonOps.mult(a, b, a);
        return A;
    }

    @Override
    public Vector mul(final Matrix A, final Vector X, final Vector Y) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F x = EJMLVectorOps.convertDVectorToDenseVector64F(X);
        final DenseMatrix64F y = EJMLVectorOps.convertDVectorToDenseVector64F(Y);
        CommonOps.mult(a, x, y);
        return Y;
    }

    @Override
    public Matrix scale(final double alpha, final Matrix A) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.scale(alpha, a);
        return A;
    }

    @Override
    public Matrix transpose(final Matrix A) {
        CommonOps.transpose(convertDMatrixToDenseMatrix64F(A));
        return A;
    }

    @Override
    public Matrix transpose(final Matrix B, final Matrix A) {
        CommonOps.transpose(convertDMatrixToDenseMatrix64F(A), convertDMatrixToDenseMatrix64F(B));
        return B;
    }

    @Override
    public boolean invert(final Matrix A) {
        return CommonOps.invert(convertDMatrixToDenseMatrix64F(A));
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    private static DenseMatrix64F convertDMatrixToDenseMatrix64F(final Matrix matrix) {
        return new DenseMatrix64F((int)matrix.numRows(), (int)matrix.numCols(), true, matrix.toArray());
    }
}
