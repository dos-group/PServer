package de.tuberlin.pserver.runtime.partitioning.mtxentries;

public abstract class AbstractMatrixEntry<V extends Number> implements MatrixEntry<V> {

    @Override
    public String toString() {
        return "(" + getRow() + ", " + getCol() + ", " + getValue() + ")";
    }
}
