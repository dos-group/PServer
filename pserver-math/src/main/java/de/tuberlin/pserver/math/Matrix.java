package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.stuff.DoubleDoubleFunction;
import de.tuberlin.pserver.math.stuff.DoubleFunction;
import de.tuberlin.pserver.math.stuff.VectorFunction;

public interface Matrix extends MObject {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum Format {

        SPARSE_MATRIX,

        DENSE_MATRIX
    }

    public enum Layout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    public static class PartitionShape {

        final long rows;
        final long cols;
        final long rowOffset;
        final long colOffset;

        public PartitionShape(long rows, long cols) {
            this(rows, cols, 0, 0);
        }

        public PartitionShape(long rows, long cols, long rowOffset, long colOffset) {
            this.rows = rows;
            this.cols = cols;
            this.rowOffset = rowOffset;
            this.colOffset = colOffset;
        }

        public long getRows() {
            return rows;
        }

        public long getCols() {
            return cols;
        }

        public long getRowOffset() {
            return rowOffset;
        }

        public long getColOffset() {
            return colOffset;
        }

        public PartitionShape create(long row, long col) {
            return new PartitionShape(row, col);
        }

        public boolean contains(long row, long col) {
            return row < rows && col < cols;
        }
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

    public abstract void lock();

    public abstract void unlock();

    public Layout getLayout();

    // ---------------------------------------------------

    public abstract long numRows();

    public abstract long numCols();

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract double atomicGet(final long row, final long col);

    public abstract void atomicSet(final long row, final long col, final double value);

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
