package de.tuberlin.pserver.math;

public interface Matrix {

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

    public abstract long numRows();

    public abstract long numCols();

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract double[] toArray();

    public abstract void setArray(final double[] data);

    public abstract RowIterator rowIterator();

    public abstract RowIterator rowIterator(final int startRow, final int endRow);

    // ---------------------------------------------------

    public abstract double aggregate(final DoubleDoubleFunction combiner, final DoubleFunction mapper);

    public abstract Vector aggregateRows(final VectorFunction f);


    public abstract Matrix add(final Matrix B);                     // A = B + A

    public abstract Matrix sub(final Matrix B);                     // A = B - A

    public abstract Matrix mul(final Matrix B);                     // A = B * A

    public abstract Vector mul(final Vector B);                     //

    public abstract Vector mul(final Vector x, final Vector y);     // y = A * x

    public abstract Matrix scale(final double alpha);               // A = alpha * A

    public abstract Matrix transpose();                             // A = A^T

    public abstract Matrix transpose(final Matrix B);               // B = A^T

    public abstract boolean invert();                               // A = A^-1

    // ---------------------------------------------------

    public abstract Matrix assign(final Matrix v);

    public abstract Matrix assign(final double v);

    public abstract Vector viewRow(final long row);

    public abstract Vector viewColumn(final long col);

    public abstract Matrix assignRow(final long row, final Vector v);

    public abstract Matrix assignColumn(final long col, final Vector v);
}
