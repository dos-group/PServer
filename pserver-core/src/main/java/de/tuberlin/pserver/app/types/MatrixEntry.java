package de.tuberlin.pserver.app.types;

/**
 * Represents a matrix entry with row, cols and value.
 */
public interface MatrixEntry {

    public long getRow();

    public long getCol();

    public double getValue();

}
