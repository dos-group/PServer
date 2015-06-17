package de.tuberlin.pserver.math.delegates.sparse.mtj;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.*;
import de.tuberlin.pserver.math.AbstractVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.sparse.*;

public class MTJMatrixOps implements LibraryMatrixOps<Matrix, Vector> {

    @Override
    public Matrix axpy(double alpha, Matrix B, Matrix A) {
        return null;
    }

    @Override
    public Matrix add(Matrix B, Matrix A) {
        return toPserverMatrix(toLibMatrix(B, true).add(toLibMatrix(A)));
    }

    @Override
    public Matrix sub(Matrix B, Matrix A) {
        return toPserverMatrix(toLibMatrix(B, true).scale(-1.).add(toLibMatrix(A)).scale(-1.));
    }

    @Override
    public Vector mul(Matrix A, Vector x) {
        SparseVector result = new SparseVector(Utils.toInt(A.numRows()), Utils.toInt(A.numRows() / 4));
        return toPserverVector(
                toLibMatrix(A, true).mult(
                        toLibVector(x),
                        result),
                Vector.VectorType.COLUMN_VECTOR);
    }

    @Override
    public Matrix mul(Matrix A, Matrix B) {
        return toPserverMatrix(
                toLibMatrix(A, true).mult(
                        toLibMatrix(B),
                        new FlexCompColMatrix(Utils.toInt(A.numRows()), Utils.toInt(B.numCols()))
                ));
    }

    @Override
    public void mul(Matrix A, Vector x, Vector y) {
        Vector result = toPserverVector(
                toLibMatrix(A).mult(
                        toLibVector(x),
                        toLibVector(y)
                ),
                Vector.VectorType.COLUMN_VECTOR);
        y.assign(result);
    }

    @Override
    public Matrix scale(double alpha, Matrix A) {
        return toPserverMatrix(toLibMatrix(A).scale(alpha));
    }

    @Override
    public Matrix transpose(Matrix A) {
        return toPserverMatrix(toLibMatrix(A, true).transpose());
    }

    @Override
    public void transpose(Matrix A, Matrix B) {
        B.assign(toPserverMatrix(toLibMatrix(A).transpose(toLibMatrix(B))));
    }

    @Override
    public boolean invert(Matrix A) {
        DenseMatrix I = Matrices.identity(Utils.toInt(Math.min(A.numCols(), A.numRows())));
        DenseMatrix AI = I.copy();
        try {
            toLibMatrix(A).solve(I, AI);
        }
        catch(MatrixSingularException e) {
            return false;
        }
        toLibMatrix(A).set(AI);
        return true;
    }

    private static no.uib.cipr.matrix.Matrix toLibMatrix(Matrix mat) {
        return toLibMatrix(mat, false);
    }

    private static no.uib.cipr.matrix.Matrix toLibMatrix(Matrix mat, boolean mutable) {
        no.uib.cipr.matrix.Matrix result = null;
        if(mat instanceof SMatrix) {
            result = ((SMatrix) mat).getContainer();
            if(mutable) {
                if(result instanceof CompRowMatrix) {
                    result = new FlexCompRowMatrix(result);
                }
                else if(result instanceof CompColMatrix) {
                    result = new FlexCompColMatrix(result);
                }
            }
        }
        else if(mat instanceof DMatrix) {
            switch(mat.getLayout()) {
                case COLUMN_LAYOUT :
                    result = new DenseMatrix(Utils.toInt(mat.numRows()), Utils.toInt(mat.numCols()), mat.toArray(), mutable);
                    break;
                case ROW_LAYOUT :
                    Matrix aux = new DMatrix(mat.numCols(), mat.numRows());
                    mat.transpose(aux);
                    result = new DenseMatrix(Utils.toInt(mat.numRows()), Utils.toInt(mat.numCols()), aux.toArray(), mutable);
                    break;
                default :
                    throw new IllegalArgumentException("Unkown memory layout: " + mat.getLayout().toString());
            }
        }
        Preconditions.checkState(result != null, "Unable to convert matrix");
        return result;
    }

    private static Matrix toPserverMatrix(no.uib.cipr.matrix.Matrix mat) {
        Matrix result = null;
        if(mat instanceof no.uib.cipr.matrix.DenseMatrix) {
            result = new DMatrix(mat.numRows(), mat.numRows(), ((DenseMatrix) mat).getData(), Matrix.MemoryLayout.COLUMN_LAYOUT);
        }
        else {
            if(mat instanceof FlexCompRowMatrix || mat instanceof CompRowMatrix) {
                result = new SMatrix(mat, Matrix.MemoryLayout.ROW_LAYOUT);
            }
            else if(mat instanceof FlexCompColMatrix || mat instanceof CompColMatrix) {
                result = new SMatrix(mat, Matrix.MemoryLayout.COLUMN_LAYOUT);
            }
        }
        Preconditions.checkState(result != null, "Unable to convert matrix");
        return result;
    }


    private static no.uib.cipr.matrix.Vector toLibVector(Vector vec) {
        no.uib.cipr.matrix.Vector result = null;
        if(vec instanceof DVector) {
            result = new DenseVector(vec.toArray());
        }
        else if(vec instanceof SVector) {
            result = ((SVector) vec).getContainer();
        }
        Preconditions.checkState(result != null, "Unable to convert vector");
        return result;
    }

    private static Vector toPserverVector(no.uib.cipr.matrix.Vector vec, Vector.VectorType type) {
        Vector result = null;
        if(MTJUtils.isDense(vec)) {
            result = new DVector(vec.size(), ((AbstractVector) vec).toArray(), type);
        }
        else {
            result = new SVector(vec.size(), ((SparseVector) vec).getIndex(), ((SparseVector) vec).getData(), type);
        }
        // Preconditions.checkState(result != null, "Unable to convert vector");
        return result;
    }


}
