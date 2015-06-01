package de.tuberlin.pserver.math.delegates.dense.ejml;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.*;
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
        DenseMatrix64F c = DenseMatrix64F.wrap((int)A.numRows(), 1, new double[(int)A.numRows()]);
        MatrixVectorMult.mult(a, b, c);
        return new DVector(a.getNumRows(), c.getData());
    }

    @Override
    public Matrix mul(final Matrix A, final Matrix B) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        DenseMatrix64F c = DenseMatrix64F.wrap((int)A.numRows(), (int)B.numCols(), new double[(int)A.numRows()*(int)B.numCols()]);
        CommonOps.mult(a, b, c);
        return new DMatrix(A.numRows(), B.numCols(), c.getData());
    }

    @Override
    public void mul(final Matrix A, final Vector X, final Vector Y) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = EJMLVectorOps.convertDVectorToDenseVector64F(X);
        final DenseMatrix64F c = EJMLVectorOps.convertDVectorToDenseVector64F(Y);
        MatrixVectorMult.mult(a, b, c);
    }

    @Override
    public Matrix scale(final double alpha, final Matrix A) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        CommonOps.scale(alpha, a);
        return A;
    }

    @Override
    public Matrix transpose(final Matrix A) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        // if matrix is square, EJML does not change the buffer. Dimensions stay the same also, so we can return the same object
        if(A.numRows() == A.numCols()) {
            TransposeAlgs.square(a);
            return A;
        }
        // however, if the matrix is not square, dimensions and buffer change. So let's create a new object
        else {
            // (also EJML requires a second buffer for cpu cache line optimization. worth it?)
            final DenseMatrix64F b = DenseMatrix64F.wrap((int)A.numCols(), (int)A.numRows(), new double[A.toArray().length]);
            DenseMatrix64F res = CommonOps.transpose(a,b);
            return new DMatrix(A.numCols(), A.numRows(), res.getData());
        }
    }

    @Override
    public void transpose(final Matrix A, final Matrix B) {
        final DenseMatrix64F a = convertDMatrixToDenseMatrix64F(A);
        final DenseMatrix64F b = convertDMatrixToDenseMatrix64F(B);
        CommonOps.transpose(a,b);
    }

    @Override
    public boolean invert(final Matrix A) {
        return CommonOps.invert(convertDMatrixToDenseMatrix64F(A));
    }

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    private static DenseMatrix64F convertDMatrixToDenseMatrix64F(final Matrix matrix) {
        return DenseMatrix64F.wrap((int)matrix.numRows(), (int)matrix.numCols(), matrix.toArray());
    }
}
