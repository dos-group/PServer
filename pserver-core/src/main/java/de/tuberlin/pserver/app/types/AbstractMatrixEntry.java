package de.tuberlin.pserver.app.types;

public abstract class AbstractMatrixEntry implements MatrixEntry {

    @Override
    public String toString() {
        return "(" + getRow() + ", " + getCol() + ", " + getValue() + ")";
    }
}
