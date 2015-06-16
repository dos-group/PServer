package de.tuberlin.pserver.math.delegates.sparse.mtj;

import de.tuberlin.pserver.math.*;
import de.tuberlin.pserver.math.delegates.LibraryMatrixOps;
import no.uib.cipr.matrix.*;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.*;

public class MTJMatrixOps implements LibraryMatrixOps<SMatrix, SVector> {

    @Override
    public SMatrix add(SMatrix B, SMatrix A) {
        return toSMatrix(toLibMatrix(B).add(toLibMatrix(A)));
    }

    @Override
    public SMatrix sub(SMatrix B, SMatrix A) {
        return toSMatrix(toLibMatrix(B).scale(-1.).add(toLibMatrix(A)).scale(-1.));
    }

    @Override
    public SVector mul(SMatrix A, SVector x) {
        Vector result = new SparseVector(Utils.toInt(A.numRows()), Utils.toInt(A.numRows() / 4));
        Matrix mtj_A = toLibMatrix(A);
        Vector mtj_x = toLibVector(x);
        return toSVector(mtj_A.mult(mtj_x, result), de.tuberlin.pserver.math.Vector.VectorType.COLUMN_VECTOR);
    }

    @Override
    public SMatrix mul(SMatrix A, SMatrix B) {
        Matrix mtj_A = toLibMatrix(A);
        Matrix mtj_B = toLibMatrix(B);
        Matrix result = new FlexCompColMatrix(mtj_A.numRows(), mtj_B.numColumns());;
        return toSMatrix(mtj_A.mult(mtj_B, result));
    }

    @Override
    public void mul(SMatrix A, SVector x, SVector y) {
        Matrix mtj_A = toLibMatrix(A);
        Vector mtj_x = toLibVector(x);
        Vector mtj_y = toLibVector(y);
        mtj_A.mult(mtj_x, mtj_y);
    }

    @Override
    public SMatrix scale(double alpha, SMatrix A) {
        return toSMatrix(toLibMatrix(A).scale(alpha));
    }

    @Override
    public SMatrix transpose(SMatrix A) {
        return toSMatrix(toLibMatrix(A).transpose());
    }

    @Override
    public void transpose(SMatrix A, SMatrix B) {
        toLibMatrix(A).transpose(toLibMatrix(B));
    }

    @Override
    public boolean invert(SMatrix A) {
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

    private static Matrix toLibMatrix(SMatrix<? extends Matrix> mat) {
        return mat.getTarget();
    }

    private static SMatrix toSMatrix(Matrix mat) {
        return new MTJMatrix(mat.numRows(), mat.numColumns(), mat, getLayout(mat));
    }

    private static SMatrix.MemoryLayout getLayout(Matrix mat) {
        SMatrix.MemoryLayout result;
        if(mat instanceof CompRowMatrix) {
            result = SMatrix.MemoryLayout.COMPRESSED_ROW;
        }
        else if(mat instanceof FlexCompRowMatrix) {
            result = SMatrix.MemoryLayout.COMPRESSED_ROW;
        }
        else if(mat instanceof CompColMatrix) {
            result = SMatrix.MemoryLayout.COMPRESSED_COL;
        }
        else if(mat instanceof FlexCompColMatrix) {
            result = SMatrix.MemoryLayout.COMPRESSED_COL;
        }
        else {
            throw new IllegalArgumentException("Unknown Matrix type");
        }
        return result;
    }

    private static Vector toLibVector(SVector<? extends Vector> vec) {
        return vec.getTarget();
    }

    private static SVector toSVector(Vector vec, de.tuberlin.pserver.math.Vector.VectorType type) {
        return new MTJVector(vec.size(), type, vec);
    }


}
