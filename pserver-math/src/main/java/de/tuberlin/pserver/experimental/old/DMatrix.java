package de.tuberlin.pserver.experimental.old;

import java.io.Serializable;

public interface DMatrix extends IMatrixOps<DMatrix, DVector>, Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum MemoryLayout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Inner Interfaces/Classes.
    // ---------------------------------------------------

    public static interface RowIterator { // ...for ROW_LAYOUT

        public abstract boolean hasNextRow();

        public abstract void nextRow();

        public abstract double getValueOfColumn(final int col);

        public abstract void reset();

        public abstract long numRows();

        public abstract long numCols();
    }

    public static interface ColumnIterator { // ...for COLUMN_LAYOUT

        public abstract boolean hasNextColumn();

        public abstract void nextColumn();

        public abstract double getValueOfRow(final int row);

        public abstract void reset();

        public abstract long numRows();

        public abstract long numCols();
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract long numRows();

    public abstract long numCols();

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract double[] toArray();

    public abstract void setArray(final double[] data);

    public abstract RowIterator rowIterator();

    public abstract RowIterator rowIterator(final int startRow, final int endRow);
}
