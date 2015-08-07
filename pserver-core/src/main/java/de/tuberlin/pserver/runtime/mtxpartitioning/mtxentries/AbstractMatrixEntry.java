package de.tuberlin.pserver.runtime.mtxpartitioning.mtxentries;

public abstract class AbstractMatrixEntry implements MatrixEntry {

    @Override
    public String toString() {
        return "(" + getRow() + ", " + getCol() + ", " + getValue() + ")";
    }
}
