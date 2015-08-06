package de.tuberlin.pserver.math.delegates.dense.ejml;

import de.tuberlin.pserver.math.DMatrix;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import org.ejml.alg.dense.misc.TransposeAlgs;
import org.ejml.alg.dense.mult.MatrixVectorMult;
import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

public final class EJMLMatrixOps implements LibraryMatrixOps<Matrix, Vector> {

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
    public Vector mul(final Matrix A, final Vector B, final Vector C) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = EJMLVectorOps.convertDVectorToDenseVector64F(B);
        final DenseMatrix64F c = EJMLVectorOps.convertDVectorToDenseVector64F(C);
        MatrixVectorMult.mult(a, b, c);
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
        return DenseMatrix64F.wrap((int)matrix.numRows(), (int)matrix.numCols(), matrix.toArray());
    }
}
