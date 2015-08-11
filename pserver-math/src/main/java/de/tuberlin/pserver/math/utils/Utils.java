package de.tuberlin.pserver.math.utils;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;

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
        Preconditions.checkArgument(data.length == rows * cols, "Can not transpose buffer: Invalid input length (%d). Must be equal to rows * cols (%d * %d = %d)", data.length, rows, cols, rows * cols);
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
     * Checks if two vectors are of the same shape.
     * @param A of shape m
     * @param B of shape n
     * @throws IncompatibleShapeException if not m == n
     */
    public static void checkShapeEqual(Vector A, Vector B) {
        if( ! (A.length() == B.length())) {
            throw new IncompatibleShapeException("Required: A: m, B: m. Given: A: %d, B: %d", A.length(), B.length());
        }
    }

    /**
     * Checks if three vectors are of the same shape.
     * @param A of shape m
     * @param B of shape n
     * @param C of shape o
     * @throws IncompatibleShapeException if not m == n == o
     */
    public static void checkShapeEqual(Vector A, Vector B, Vector C) {
        if( ! (A.length() == B.length() && A.length() == C.length())) {
            throw new IncompatibleShapeException("Required: A: m, B: m, C: m. Given: A: %d, B: %d, C: %d", A.length(), B.length(), C.length());
        }
    }

    /**
     * Checks if two matrices are of the same shape.
     * @param A of shape m x n
     * @param B of shape o x p
     * @throws IncompatibleShapeException if not (m == o && n == p)
     */
    public static void checkShapeEqual(Matrix A, Matrix B) {
        if( ! (A.numRows() == B.numRows() && A.numCols() == B.numCols())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: m x n. Given: A: %d x %d, B: %d x %d", A.numRows(), A.numCols(), B.numRows(), B.numCols());
        }
    }

    /**
     * Checks if three matrices are of the same shape.
     * @param A of shape m x n
     * @param B of shape o x p
     * @param C of shape q x e
     * @throws IncompatibleShapeException if not (m == o == q && n == p == e)
     */
    public static void checkShapeEqual(Matrix A, Matrix B, Matrix C) {
        if( ! (A.numRows() == B.numRows() && A.numRows() == C.numRows() && A.numCols() == B.numCols() && A.numCols() == C.numCols())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: m x n, B: m x n. Given: A: %d x %d, B: %d x %d, C: %d x %d", A.numRows(), A.numCols(), B.numRows(), B.numCols(), C.numRows(), C.numCols());
        }
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
     * @throws IncompatibleShapeException if not (n == o && m == r)
     */
    public static void checkShapeMatrixMatrixMult(Matrix A, Matrix B, Matrix C) {
        if( ! (A.numCols() == B.numRows() && A.numRows() == C.numRows() && B.numCols() == C.numCols())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: n x o, C: m x o. Given: A: %d x %d, B: %d x %d, C: %d x %d", A.numRows(), A.numCols(), B.numRows(), B.numCols(), C.numRows(), C.numCols());
        }
    }

    /**
     * Checks if A, b and c have valid shapes to multiply A and b and store the result in c.
     * @param A of shape m x n
     * @param b of size  o
     * @param c of size  p
     * @throws IncompatibleShapeException if not (n == o && m == p)
     */
    public static void checkShapeMatrixVectorMult(Matrix A, Vector b, Vector c) {
        if( ! (A.numCols() == b.length() && A.numRows() == c.length())) {
            throw new IncompatibleShapeException("Required: A: m x n, b: n, c: m. Given: A: %d x %d, b: %d, c: %d", A.numRows(), A.numCols(), b.length(), c.length());
        }
    }

    /**
     * Checks if A is square
     * @param A of shape m x n
     * @throws IncompatibleShapeException if not (m == n)
     */
    public static void checkShapeSquare(Matrix A) {
        if( ! (A.numRows() == A.numCols())) {
            throw new IncompatibleShapeException("Required: A: m x m. Given: A: %d x %d", A.numRows(), A.numCols());
        }
    }

    /**
     * Checks if B can store A transposed.
     * @param A of shape m x n
     * @param A of shape o x p
     * @throws IncompatibleShapeException if not (m == p && n == o)
     */
    public static void checkShapeTranspose(Matrix A, Matrix B) {
        if( ! (A.numRows() == B.numCols() && A.numCols() == B.numRows())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: n x m. Given: A: %d x %d, B: %d x %d", A.numRows(), A.numCols(), B.numRows(), B.numCols());
        }
    }

    /**
     * Checks if v can be applied to the rows of A, and if the result can be stored in B.
     * @param A of shape m x n
     * @param v of shape o
     * @param B of shape p x q
     * @throws IncompatibleShapeException if not (n == o && m == p && n == q)
     */
    public static void checkApplyVectorToRows(Matrix A, Vector v, Matrix B) {
        if( ! (A.numRows() == B.numRows() && A.numCols() == B.numCols()) && A.numCols() == v.length()) {
            throw new IncompatibleShapeException("Required: A: m x n, v: n, B: m x n. Given: A: %d x %d, v: %d, B: %d x %d", A.numRows(), A.numCols(), v.length(), B.numRows(), B.numCols());
        }
    }

    /**
     * Checks if v can be applied to the cols of A, and if the result can be stored in B.
     * @param A of shape m x n
     * @param v of shape o
     * @param B of shape p x q
     * @throws IncompatibleShapeException if not (m == o && m == p && n == q)
     */
    public static void checkApplyVectorToCols(Matrix A, Vector v, Matrix B) {
        if( ! (A.numRows() == B.numRows() && A.numCols() == B.numCols()) && A.numRows() == v.length()) {
            throw new IncompatibleShapeException("Required: A: m x n, v: m, B: m x n. Given: A: %d x %d, v: %d, B: %d x %d", A.numRows(), A.numCols(), v.length(), B.numRows(), B.numCols());
        }
    }
}
