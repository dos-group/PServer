package de.tuberlin.pserver.matrix;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.matrix.radt.S3Vector;

// Todo: don't really want to implement Operation...
public class MatrixOperation<T> implements Operation{
    public enum MatrixOpType {
        SET,
        SET_DIAGONALS_ZERO,
        TRANSPOSE,
        AVERAGE
    }
    private final S3Vector s3Vector;
    private final long[] vectorClock;
    private final long row;
    private final long col;
    private final T value;
    private final MatrixOpType opType;

    public MatrixOperation(T value, long row, long col, MatrixOpType opType, long[] vectorClock, S3Vector s3Vector) {
        this.s3Vector = s3Vector;
        this.row = row;
        this.col = col;
        this.value = value;
        this.opType = opType;
        this.vectorClock = vectorClock;
    }

    public S3Vector getS3Vector() {
        return s3Vector;
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

    public long[] getVectorClock() {
        return vectorClock;
    }
}
