package de.tuberlin.pserver.math.utils;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.matrix.MatrixBase;

public class Utils {

    public static final double DEFAULT_EPSILON = 0.001;

    public static int getPos(final long row, final long col, MatrixBase mat) {
        return getPos(row, col, mat.rows(), mat.cols());
    }

    public static int getPos(final long row, final long col, long numRows, long numCols) {
        return toInt(row * numCols + col);
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

    /*public static double[] transposeBufferInplace(double[] data, int rows, int cols, Layout layout) {
        Preconditions.checkArgument(data.length == rows * cols, "Can not transpose buffer: Invalid input length (%d). Must be equal to rows * cols (%d * %d = %d)", data.length, rows, cols, rows * cols);
        for(int i = 0; i < rows; i++) {
            for(int j = 0; j < cols; j++) {
                double tmp = data[Utils.getPos(i, j, layout, rows, cols)];
                data[Utils.getPos(i, j, layout, rows, cols)] = data[Utils.getPos(j, i, layout, rows, cols)];
                data[Utils.getPos(j, i, layout, rows, cols)] = tmp;
            }
        }
        return data;
    }*/

    /**
     * Checks if two matrices are of the same shape.
     * @param A of shape m x n
     * @param B of shape o x p
     * @throws IncompatibleShapeException if not (m == o && n == p)
     */
    public static void checkShapeEqual(MatrixBase A, MatrixBase B) {
        if( ! (A.rows() == B.rows() && A.cols() == B.cols())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: m x n. Given: A: %d x %d, B: %d x %d", A.rows(), A.cols(), B.rows(), B.cols());
        }
    }

    /**
     * Checks if three matrices are of the same shape.
     * @param A of shape m x n
     * @param B of shape o x p
     * @param C of shape q x e
     * @throws IncompatibleShapeException if not (m == o == q && n == p == e)
     */
    public static void checkShapeEqual(MatrixBase A, MatrixBase B, MatrixBase C) {
        if( ! (A.rows() == B.rows() && A.rows() == C.rows() && A.cols() == B.cols() && A.cols() == C.cols())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: m x n, B: m x n. Given: A: %d x %d, B: %d x %d, C: %d x %d", A.rows(), A.cols(), B.rows(), B.cols(), C.rows(), C.cols());
        }
    }

    /**
     * Checks if two matrices can be multiplied.
     * @param A of shape m x n
     * @param B of shape o x p
     * @return true iff n == o
     */
    public static boolean shapeMul2(MatrixBase A, MatrixBase B) {
        return A.cols() == B.rows();
    }

    /**
     * Checks if A, B and C have valid shapes to multiply A and B and store the result in C.
     * @param A of shape m x n
     * @param B of shape o x p
     * @param C of shape q x r
     * @throws IncompatibleShapeException if not (n == o && m == r)
     */
    public static void checkShapeMatrixMatrixMult(MatrixBase A, MatrixBase B, MatrixBase C) {
        if( ! (A.cols() == B.rows() && A.rows() == C.rows() && B.cols() == C.cols())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: n x o, C: m x o. Given: A: %d x %d, B: %d x %d, C: %d x %d", A.rows(), A.cols(), B.rows(), B.cols(), C.rows(), C.cols());
        }
    }

    /**
     * Checks if A is square
     * @param A of shape m x n
     * @throws IncompatibleShapeException if not (m == n)
     */
    public static void checkShapeSquare(MatrixBase A) {
        if( ! (A.rows() == A.cols())) {
            throw new IncompatibleShapeException("Required: A: m x m. Given: A: %d x %d", A.rows(), A.cols());
        }
    }

    /**
     * Checks if B can store A transposed.
     * @param A of shape m x n
     * @param A of shape o x p
     * @throws IncompatibleShapeException if not (m == p && n == o)
     */
    public static void checkShapeTranspose(MatrixBase A, MatrixBase B) {
        if( ! (A.rows() == B.cols() && A.cols() == B.rows())) {
            throw new IncompatibleShapeException("Required: A: m x n, B: n x m. Given: A: %d x %d, B: %d x %d", A.rows(), A.cols(), B.rows(), B.cols());
        }
    }

    /**
     * Checks if v can be applied to the rows of A, and if the result can be stored in B.
     * @param A of shape m x n
     * @param V of shape 1 x o
     * @param B of shape p x q
     * @throws IncompatibleShapeException if not (n == o && m == p && n == q)
     */
    public static void checkApplyVectorToRows(MatrixBase A, MatrixBase V, MatrixBase B) {
        if( ! (A.rows() == B.rows() && A.cols() == B.cols()) && A.cols() == V.cols() && V.rows() == 1) {
            throw new IncompatibleShapeException("Required: A: m x n, V: 1 x n, B: m x n. Given: A: %d x %d, V: %d x %d, B: %d x %d", A.rows(), A.cols(), V.rows(), V.cols(), B.rows(), B.cols());
        }
    }

    /**
     * Checks if v can be applied to the cols of A, and if the result can be stored in B.
     * @param A of shape m x n
     * @param V of shape o x 1
     * @param B of shape p x q
     * @throws IncompatibleShapeException if not (m == o && m == p && n == q)
     */
    public static void checkApplyVectorToCols(MatrixBase A, MatrixBase V, MatrixBase B) {
        if( ! (A.rows() == B.rows() && A.cols() == B.cols()) && A.rows() == V.rows() && V.cols() == 1) {
            throw new IncompatibleShapeException("Required: A: m x n, V: m x 1, B: m x n. Given: A: %d x %d, V: %d x %d, B: %d x %d", A.rows(), A.cols(), V.rows(), V.cols(), B.rows(), B.cols());
        }
    }

    /**
     * Checks if dot product is possible between vector A and B. Right now only dot products between
     * vectors with same layout is possible
     * @param A of shape m x 1 or 1 x m
     * @param B of shape m x 1 or 1 x m
     * @throws IncompatibleShapeException if not A.dot(B)
     */
    public static void checkDotProduct(MatrixBase A, MatrixBase B) {
        if( ! (A.rows() == B.rows() && A.cols() == B.cols())) {
            throw new IncompatibleShapeException("Required: A: 1 x m, B: 1 x m. " +
                    "Given: A: %d x %d, B: %d x %d", A.rows(), A.cols(), B.rows(), B.cols());
        } else {
            if (A.rows() != 1 || A.cols() != 1) {
                throw new IncompatibleShapeException("A and B must be vectors. " +
                        "Given: A: %d x %d, B: %d x %d", A.rows(), A.cols(), B.rows(), B.cols());
            }
        }
    }
}
