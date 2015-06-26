package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;

public class Utils {

    public static final double DEFAULT_EPSILON = 0.0000001;

    public static int getPos(final long row, final long col, Matrix mat) {
        return getPos(row, col, mat.getLayout(), mat.numRows(), mat.numCols());
    }

    public static int getPos(final long row, final long col, Matrix.MemoryLayout layout, long numRows, long numCols) {
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

    public static double[] transposeBufferInplace(double[] data, int rows, int cols, Matrix.MemoryLayout layout) {
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

}
