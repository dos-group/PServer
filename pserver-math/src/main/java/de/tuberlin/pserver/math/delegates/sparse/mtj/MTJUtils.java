package de.tuberlin.pserver.math.delegates.sparse.mtj;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.*;
import de.tuberlin.pserver.math.stuff.Utils;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.*;

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

    public static int[][] buildRowBasedNz(DMatrix mat) {
        return buildRowBasedNz(mat.toArray(), mat.numRows(), mat.numCols(), mat.getLayout());
    }

    public static int[][] buildColBasedNz(DMatrix mat) {
        return buildColBasedNz(mat.toArray(), mat.numRows(), mat.numCols(), mat.getLayout());
    }

    public static int[][] buildRowBasedNz(double[] data, long rows, long cols, de.tuberlin.pserver.math.Matrix.Layout layout) {
        // nz: for each col: the row-indices that are not zero
        int[][] nz = new int[Utils.toInt(rows)][];
        // (create buffer for row indices)
        int[] buffer = new int[Utils.toInt(cols)];
        // for each row
        for(int row = 0; row < Utils.toInt(rows); row++) {
            // so far 0 non-zero row-indices found
            int currentBufferIndex = 0;
            // for each col
            for(int col = 0; col < Utils.toInt(cols); col++) {
                // if element is non-zero
                if( ! Utils.closeTo(data[Utils.getPos(row, col, layout, rows, cols)], 0.0)) {
                    // add col-index to buffer and increase counter
                    buffer[currentBufferIndex++] = col;
                }
            }
            // deep-add all found non-zero col-indices of current row to nz
            nz[row] = java.util.Arrays.copyOf(buffer, currentBufferIndex);
        }
        return nz;
    }

    public static int[][] buildColBasedNz(double[] data, long rows, long cols, de.tuberlin.pserver.math.Matrix.Layout layout) {
        // nz: for each col: the row-indices that are not zero
        int[][] nz = new int[Utils.toInt(cols)][];
        // (create buffer for row indices)
        int[] buffer = new int[Utils.toInt(rows)];
        // for each col
        for(int col = 0; col < Utils.toInt(cols); col++) {
            // so far 0 non-zero row-indices found
            int currentBufferIndex = 0;
            // for each row
            for(int row = 0; row < Utils.toInt(rows); row++) {
                // if element is non-zero
                if( ! Utils.closeTo(data[Utils.getPos(row, col, layout, rows, cols)], 0.0)) {
                    // add row-index to buffer and increase counter
                    buffer[currentBufferIndex++] = row;
                }
            }
            // deep-add all found non-zero row-indices of current col to nz
            nz[col] = java.util.Arrays.copyOf(buffer, currentBufferIndex);
        }
        return nz;
    }

    public static no.uib.cipr.matrix.Matrix toLibMatrix(de.tuberlin.pserver.math.Matrix mat) {
        return toLibMatrix(mat, false);
    }

    public static no.uib.cipr.matrix.Matrix toLibMatrix(de.tuberlin.pserver.math.Matrix mat, boolean mutable) {
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
                    de.tuberlin.pserver.math.Matrix aux = new DMatrix(mat.numCols(), mat.numRows());
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

    public static de.tuberlin.pserver.math.Matrix toPserverMatrix(no.uib.cipr.matrix.Matrix mat) {
        de.tuberlin.pserver.math.Matrix result = null;
        if(mat instanceof no.uib.cipr.matrix.DenseMatrix) {
            result = new DMatrix(mat.numRows(), mat.numRows(), ((DenseMatrix) mat).getData(), de.tuberlin.pserver.math.Matrix.Layout.COLUMN_LAYOUT);
        }
        else {
            if(mat instanceof FlexCompRowMatrix || mat instanceof CompRowMatrix) {
                result = new SMatrix(mat, de.tuberlin.pserver.math.Matrix.Layout.ROW_LAYOUT);
            }
            else if(mat instanceof FlexCompColMatrix || mat instanceof CompColMatrix) {
                result = new SMatrix(mat, de.tuberlin.pserver.math.Matrix.Layout.COLUMN_LAYOUT);
            }
        }
        Preconditions.checkState(result != null, "Unable to convert matrix");
        return result;
    }


    public static no.uib.cipr.matrix.Vector toLibVector(de.tuberlin.pserver.math.Vector vec) {
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

    public static de.tuberlin.pserver.math.Vector toPserverVector(no.uib.cipr.matrix.Vector vec, de.tuberlin.pserver.math.Vector.Layout type) {
        de.tuberlin.pserver.math.Vector result = null;
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
