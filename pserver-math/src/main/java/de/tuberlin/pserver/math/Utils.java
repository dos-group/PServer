package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;

public class Utils {

    public static int getPos(final long row, final long col, DMatrix.MemoryLayout layout, long numRows, long numCols) {
        switch (layout) {
            case ROW_LAYOUT: return toInt(row * numCols + col);
            case COLUMN_LAYOUT: return toInt(col * numRows + row);
        }
        throw new IllegalStateException();
    }

    public static int getPos(final long row, final long col, SMatrix.MemoryLayout layout, long numRows, long numCols) {
        switch (layout) {
            case COMPRESSED_ROW: return toInt(row * numCols + col);
            case COMPRESSED_COL: return toInt(col * numRows + row);
        }
        throw new IllegalStateException();
    }

    public static int toInt(long value) {
        Preconditions.checkArgument(value < Integer.MIN_VALUE || value > Integer.MAX_VALUE, "Parameter cannot be casted to int without changing its value.");
        return (int) value;
    }

}
