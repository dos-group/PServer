package de.tuberlin.pserver.app.types;

import de.tuberlin.pserver.app.PServerContext;

import java.io.Serializable;

public interface DMatrix extends Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum MemoryLayout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Inner Interfaces.
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

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract long numRows();

    public abstract long numCols();

    public abstract RowIterator rowIterator(final int startRow, final int endRow);

    public abstract RowIterator rowIterator();

    public abstract double[] toArray();
}
