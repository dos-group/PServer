package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;

public class NoPartitioner extends AbstractMatrixPartitioner {

    public NoPartitioner() { this(null); }
    public NoPartitioner(MatrixTypeInfo distributedMatrixType) { super(distributedMatrixType); }

    @Override
    public int getPartitionOfEntry(long row, long col) {
        return -1;
    }

    @Override
    public long globalToLocalRow(long row) {
        return row;
    }

    @Override
    public long globalToLocalColumn(long col) {
        return col;
    }

    @Override
    public long localToGlobalRow(long row) {
        return row;
    }

    @Override
    public long localToGlobalColumn(long col) {
        return col;
    }

    @Override
    public int getNumRowPartitions() {
        return 1;
    }

    @Override
    public int getNumColPartitions() {
        return 1;
    }

    @Override
    protected MatrixPartitionShape computeMatrixPartitionShape() {
        if (distributedMatrixType != null)
            return new MatrixPartitionShape(distributedMatrixType.globalRows(), distributedMatrixType.globalCols(), 0, 0);
        else
            return null;
    }
}