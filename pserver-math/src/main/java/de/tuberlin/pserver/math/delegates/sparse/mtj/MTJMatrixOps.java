package de.tuberlin.pserver.math.delegates.sparse.mtj;

import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MTJMatrixOps implements LibraryMatrixOps<Matrix, Vector> {

    @Override
    public Matrix add(Matrix A, Matrix B, Matrix C) {
        throw new NotImplementedException();
    }

    @Override
    public Matrix sub(Matrix A, Matrix B, Matrix C) {
        throw new NotImplementedException();
    }

    @Override
    public Matrix mul(Matrix A, Matrix B, Matrix C) {
        throw new NotImplementedException();
    }

    @Override
    public Vector mul(Matrix A, Vector b, Vector c) {
        throw new NotImplementedException();
    }

    @Override
    public Matrix scale(Matrix A, double alpha, Matrix B) {
        throw new NotImplementedException();
    }

    @Override
    public Matrix transpose(Matrix A, Matrix B) {
        throw new NotImplementedException();
    }

    @Override
    public boolean invert(Matrix A, Matrix B) {
        throw new NotImplementedException();
    }

/*@Override
    public Matrix add(Matrix A, Matrix B, Matrix C) {
        return MTJUtils.toPserverMatrix(MTJUtils.toLibMatrix(B, true).add(MTJUtils.toLibMatrix(A)));
    }

    @Override
    public Matrix sub(Matrix B, Matrix A) {
        return MTJUtils.toPserverMatrix(MTJUtils.toLibMatrix(A, true).add((MTJUtils.toLibMatrix(B)).scale(-1.)));
    }

    @Override
    public Vector mul(Matrix A, Vector x) {
        SparseVector result = new SparseVector(Utils.toInt(A.numRows()), Utils.toInt(A.numRows() / 4));
        return MTJUtils.toPserverVector(
                MTJUtils.toLibMatrix(A, true).mult(
                        MTJUtils.toLibVector(x),
                        result),
                Vector.Layout.COLUMN_LAYOUT);
    }

    @Override
    public Matrix mul(Matrix A, Matrix B) {
        return MTJUtils.toPserverMatrix(
                MTJUtils.toLibMatrix(A, true).mult(
                        MTJUtils.toLibMatrix(B),
                        new FlexCompColMatrix(Utils.toInt(A.numRows()), Utils.toInt(B.numCols()))
                ));
    }

    @Override
    public void mul(Matrix A, Vector x, Vector y) {
        Vector result = MTJUtils.toPserverVector(
                MTJUtils.toLibMatrix(A).mult(
                        MTJUtils.toLibVector(x),
                        MTJUtils.toLibVector(y)
                ),
                Vector.Layout.COLUMN_LAYOUT);
        y.assign(result);
    }

    @Override
    public Matrix scale(double alpha, Matrix A) {
        return MTJUtils.toPserverMatrix(MTJUtils.toLibMatrix(A).scale(alpha));
    }

    @Override
    public Matrix transpose(Matrix A) {
        return MTJUtils.toPserverMatrix(MTJUtils.toLibMatrix(A, true).transpose());
    }

    @Override
    public void transpose(Matrix A, Matrix B) {
        B.assign(MTJUtils.toPserverMatrix(MTJUtils.toLibMatrix(A).transpose(MTJUtils.toLibMatrix(B))));
    }

    @Override
    public boolean invert(Matrix A) {
        DenseMatrix I = Matrices.identity(Utils.toInt(Math.min(A.numCols(), A.numRows())));
        DenseMatrix AI = I.copy();
        try {
            MTJUtils.toLibMatrix(A).solve(I, AI);
        }
        catch(MatrixSingularException e) {
            return false;
        }
        MTJUtils.toLibMatrix(A).set(AI);
        return true;
    }*/

}
