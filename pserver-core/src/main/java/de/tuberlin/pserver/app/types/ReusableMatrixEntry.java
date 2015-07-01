package de.tuberlin.pserver.app.types;

/**
 * Represents a reusable matrix entry with row, cols and value.
 */
public interface ReusableMatrixEntry extends MatrixEntry {

    public MatrixEntry set(long row, long col, double value);

}
