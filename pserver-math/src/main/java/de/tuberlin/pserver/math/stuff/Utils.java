package de.tuberlin.pserver.math.stuff;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

public class Utils {

    public static final double DEFAULT_EPSILON = 0.001;

    public static int getPos(final long row, final long col, Matrix mat) {
        return getPos(row, col, mat.getLayout(), mat.numRows(), mat.numCols());
    }

    public static int getPos(final long row, final long col, Matrix.Layout layout, long numRows, long numCols) {
        switch (layout) {
            case ROW_LAYOUT: return toInt(row * numCols + col);
            case COLUMN_LAYOUT: return toInt(col * numRows + row);
        }
        throw new IllegalStateException();
    }

    public static int toInt(long value) {
        Preconditions.checkArgument(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE, "Long value '%s' cannot be casted to int without changing its value.", value);
        return (int) value;
    }

    public static boolean closeTo(double val, double target) {
        return closeTo(val, target, DEFAULT_EPSILON);
    }

    public static boolean closeTo(double val, double target, double eps) {
        //    Interval: (target-eps, target+eps)
        //   (<target-eps>------target------<target+eps>)
        return val > target - eps && val < target + eps;
    }

    public static double[] transposeBufferInplace(double[] data, int rows, int cols, Matrix.Layout layout) {
        Preconditions.checkArgument(data.length   == rows*cols, "Can not transpose buffer: Invalid input length (%d). Must be equal to rows * cols (%d * %d = %d)", data.length, rows, cols, rows * cols);
        for(int i = 0; i < rows; i++) {
            for(int j = 0; j < cols; j++) {
                double tmp = data[Utils.getPos(i, j, layout, rows, cols)];
                data[Utils.getPos(i, j, layout, rows, cols)] = data[Utils.getPos(j, i, layout, rows, cols)];
                data[Utils.getPos(j, i, layout, rows, cols)] = tmp;
            }
        }
        return data;
    }

    /**
     * Checks if two matrices are of the same shape.
     * @param A of shape m x n
     * @param B of shape o x p
     * @return true iff m == o && n == p
     */
    public static boolean shapeEqual(Matrix A, Matrix B) {
        return A.numRows() == B.numRows() && A.numCols() == B.numCols();
    }

    /**
     * Checks if two matrices can be multiplied.
     * @param A of shape m x n
     * @param B of shape o x p
     * @return true iff n == o
     */
    public static boolean shapeMul2(Matrix A, Matrix B) {
        return A.numCols() == B.numRows();
    }

    /**
     * Checks if A, B and C have valid shapes to multiply A and B and store the result in C.
     * @param A of shape m x n
     * @param B of shape o x p
     * @param C of shape q x r
     * @return true iff n == o && m == r
     */
    public static boolean shapeMatrixMatrixMult(Matrix A, Matrix B, Matrix C) {
        return A.numCols() == B.numRows() && A.numRows() == C.numRows() && B.numCols() == C.numCols();
    }

    /**
     * Checks if A, b and c have valid shapes to multiply A and b and store the result in c.
     * @param A of shape m x n
     * @param b of size  o
     * @param c of size  p
     * @return true iff n == o && m == p
     */
    public static boolean shapeMatrixVectorMult(Matrix A, Vector b, Vector c) {
        return A.numCols() == b.length() && A.numRows() == c.length();
    }

    /**
     * Checks if A is square
     * @param A of shape m x n
     * @return true iff m == n
     */
    public static boolean shapeSquare(Matrix A) {
        return A.numRows() == A.numCols();
    }

    /**
     * Checks if B can store A transposed.
     * @param A of shape m x n
     * @param A of shape o x p
     * @return true iff m == p && n == o
     */
    public static boolean shapeTranspose(Matrix A, Matrix B) {
        return A.numRows() == B.numCols() && A.numCols() == B.numRows();
    }

}
