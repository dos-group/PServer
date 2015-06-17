package de.tuberlin.pserver.math.delegates.sparse.mtj;

import de.tuberlin.pserver.math.AbstractMatrix;
import de.tuberlin.pserver.math.Utils;
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

    public static int[][] buildRowBasedNz(double[] data, long rows, long cols, AbstractMatrix.MemoryLayout layout) {
        int[][] nz = new int[Utils.toInt(rows)][];
        for(long i = 0; i < rows; i++) {
            int[] buffer = new int[Utils.toInt(cols)];
            int bufLength = 0;
            for(long j = 0; j < cols; j++) {
                if(Utils.closeTo(data[Utils.getPos(i, j, layout, rows, cols)], 0.0)) {
                    buffer[bufLength] = Utils.toInt(j);
                    bufLength++;
                }
            }
            nz[Utils.toInt(i)] = java.util.Arrays.copyOf(buffer, bufLength);
        }
        return nz;
    }

    public static int[][] buildColBasedNz(double[] data, long rows, long cols, AbstractMatrix.MemoryLayout layout) {
        int[][] nz = new int[Utils.toInt(cols)][];
        for(long i = 0; i < cols; i++) {
            int[] buffer = new int[Utils.toInt(rows)];
            int bufLength = 0;
            for(long j = 0; j < rows; j++) {
                if(Utils.closeTo(data[Utils.getPos(i, j, layout, rows, cols)], 0.0)) {
                    buffer[bufLength] = Utils.toInt(j);
                    bufLength++;
                }
            }
            nz[Utils.toInt(i)] = java.util.Arrays.copyOf(buffer, bufLength);
        }
        return nz;
    }

}
