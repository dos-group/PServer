package de.tuberlin.pserver.matrix.crdt;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.matrix.radt.S3Vector;

// Todo: don't really want to implement Operation...
public  class MatrixAvgOperation64F<T> implements Operation {
    public enum MatrixOpType {
        AVERAGE
    }

    private final double sessionID;
    private final long row;
    private final long col;
    private final T value;
    private final MatrixOpType opType;

    public MatrixAvgOperation64F(T value, long row, long col, MatrixOpType opType, double sessionID) {
        this.row = row;
        this.col = col;
        this.value = value;
        this.opType = opType;
        this.sessionID = sessionID;
    }

    public double getSessionID() {
        return sessionID;
    }

    public long getRow() {
        return row;
    }

    public long getCol() {
        return col;
    }

    // TODO: this isn't functional, just a remnanat
    @Override
    public OpType getType() {
        return null;
    }

    public T getValue() {
        return value;
    }

    public MatrixOpType getOpType() {
        return opType;
    }

}
