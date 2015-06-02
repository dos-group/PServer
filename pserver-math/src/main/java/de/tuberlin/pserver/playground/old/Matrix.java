package de.tuberlin.pserver.playground.old;

public interface Matrix {

    public abstract long numRows();

    public abstract long numCols();

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract Matrix assign(final Matrix v);

    public abstract Matrix assign(final double v);

    public abstract Vector viewRow(final long row);

    public abstract Vector viewColumn(final long col);

    public abstract Matrix assignRow(final long row, final Vector v);

    public abstract Matrix assignColumn(final long col, final Vector v);
}
