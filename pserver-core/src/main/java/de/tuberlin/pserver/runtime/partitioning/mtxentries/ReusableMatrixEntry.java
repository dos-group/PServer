package de.tuberlin.pserver.runtime.partitioning.mtxentries;

/**
 * Represents a reusable matrix entry with row, cols and value.
 */
public interface ReusableMatrixEntry<V extends Number> extends MatrixEntry<V> {

    public MatrixEntry set(long row, long col, V value);

}
