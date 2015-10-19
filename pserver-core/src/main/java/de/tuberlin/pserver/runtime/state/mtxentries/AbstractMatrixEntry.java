package de.tuberlin.pserver.runtime.state.mtxentries;

public abstract class AbstractMatrixEntry<V extends Number> implements MatrixEntry<V> {

    @Override
    public String toString() {
        return "(" + getRow() + ", " + getCol() + ", " + getValue() + ")";
    }
}
