package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;


public class MatrixVirtualRowPartitioner extends AbstractMatrixPartitioner {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixVirtualRowPartitioner() {};

    public MatrixVirtualRowPartitioner(MatrixTypeInfo distributedMatrixType) {
        super(distributedMatrixType);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public int getPartitionOfEntry(long row, long col) { return distributedMatrixType.nodeId(); }

    @Override public long globalToLocalRow(long row) { return row; }

    @Override public long globalToLocalColumn(long col) { return col; }

    @Override public long localToGlobalRow(long row) { return row; }

    @Override public long localToGlobalColumn(long col) { return col; }

    @Override public int getNumRowPartitions() { return 1; }

    @Override public int getNumColPartitions() { return 1; }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected MatrixPartitionShape computeMatrixPartitionShape() {
        return new MatrixPartitionShape(distributedMatrixType.globalRows(), distributedMatrixType.globalCols(), 0, 0);
    }
}
