package de.tuberlin.pserver.math.delegates.dense.ejml;

import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.dense.DVector;
//import org.ejml.alg.dense.mult.MatrixVectorMult;
//import org.ejml.data.DenseMatrix64F;
//import org.ejml.ops.CommonOps;

public final class EJMLMatrixOps implements LibraryMatrixOps<Matrix, Vector> {
    @Override
    public Matrix add(Matrix A, Matrix B, Matrix C) {
        return null;
    }

    @Override
    public Matrix sub(Matrix A, Matrix B, Matrix C) {
        return null;
    }

    @Override
    public Matrix mul(Matrix A, Matrix B, Matrix C) {
        return null;
    }

    @Override
    public Vector mul(Matrix A, Vector b, Vector c) {
        return null;
    }

    @Override
    public Matrix scale(Matrix A, double alpha, Matrix B) {
        return null;
    }

    @Override
    public Matrix transpose(Matrix A, Matrix B) {
        return null;
    }

    @Override
    public boolean invert(Matrix A, Matrix B) {
        return false;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------
    /*
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
    }*/

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    //private static DenseMatrix64F convertDMatrixToDenseMatrix64F(final Matrix matrix) {
    //    return DenseMatrix64F.wrap((int)matrix.numRows(), (int)matrix.numCols(), matrix.toArray());
    //}
}
