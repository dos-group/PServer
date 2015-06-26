package de.tuberlin.pserver.math;

import java.io.Serializable;

public interface Matrix extends Serializable {

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

        public abstract void nextRandomRow();

        public abstract double getValueOfColumn(final int col);

        public abstract Vector getAsVector();

        public abstract Vector getAsVector(int from, int size);

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

    public abstract void setOwner(final Object owner);

    public abstract Object getOwner();

    public abstract void lock();

    public abstract void unlock();

    public MemoryLayout getLayout();

    // ---------------------------------------------------

    public abstract long numRows();

    public abstract long numCols();

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract double atomicGet(final long row, final long col);

    public abstract void atomicSet(final long row, final long col, final double value);

    public abstract double[] toArray();

    public abstract void setArray(final double[] data);

    public abstract RowIterator rowIterator();

    public abstract RowIterator rowIterator(final int startRow, final int endRow);

    // ---------------------------------------------------

    public abstract double aggregate(final DoubleDoubleFunction combiner, final DoubleFunction mapper);

    public abstract Vector aggregateRows(final VectorFunction f);

    public abstract Matrix axpy(final double alpha, final Matrix B);        // A = alpha * B + A

    public abstract Matrix add(final Matrix B);                             // A = B + A

    public abstract Matrix sub(final Matrix B);                                             // A = B - A

    public abstract Matrix mul(final Matrix B);                                             // A = B * A

    public abstract Vector mul(final Vector B);                                             //

    public abstract void mul(final Vector x, final Vector y);                               // y = A * x

    public abstract Matrix scale(final double alpha);                                       // A = alpha * A

    public abstract Matrix transpose();                                                     // A = A^T

    public abstract void transpose(final Matrix B);                                         // B = A^T

    public abstract boolean invert();                                                       // A = A^-1

    // ---------------------------------------------------

    public Vector rowAsVector();

    public Vector rowAsVector(final long row);

    public Vector rowAsVector(final long row, final long from, final long to);

    public Vector colAsVector();

    public Vector colAsVector(final long col);

    public Vector colAsVector(final long col, final long from, final long to);

    public abstract Matrix assign(final Matrix v);

    public abstract Matrix assign(final double v);

    public abstract Matrix assignRow(final long row, final Vector v);

    public abstract Matrix assignColumn(final long col, final Vector v);

    public abstract Matrix copy();
}
