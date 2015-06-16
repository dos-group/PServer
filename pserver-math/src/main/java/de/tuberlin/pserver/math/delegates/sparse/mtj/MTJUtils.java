package de.tuberlin.pserver.math.delegates.sparse.mtj;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.ISparseVector;

public class MTJUtils {

    public static boolean isSparse(Matrix m) {
        return !(m instanceof DenseMatrix);
    }

    public static boolean isDense(Matrix m) {
        return m instanceof DenseMatrix;
    }

    public static boolean isSparse(Vector v) {
        return v instanceof ISparseVector;
    }

    public static boolean isDense(Vector v) {
        return ! (v instanceof ISparseVector);
    }

}
