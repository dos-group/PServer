package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.implementation.f32.entries.Entry32F;
import de.tuberlin.pserver.types.matrix.metadata.DistributedMatrixType;

public class MatrixRowPartitioner extends AbstractMatrixPartitioner {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixRowPartitioner(DistributedMatrixType distributedMatrixType) {
        super(distributedMatrixType);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int getPartitionOfEntry(final Entry32F entry) {
        double numOfRowsPerInstance = (double) distributedMatrixType.rows() / distributedMatrixType.nodes().length;
        int partition = (int) (entry.getRow() / numOfRowsPerInstance);
        if(partition >= distributedMatrixType.nodes().length) {
            throw new IllegalStateException("The calculated partition id (row = " + entry.getRow() + ", distributedMatrixType.rows() = "+distributedMatrixType.rows()+", numNodes = "+distributedMatrixType.nodes().length+") -> " + partition + " must not exceed numNodes.");
        }
        return distributedMatrixType.nodes()[partition];
    }

    @Override
    public int getPartitionOfEntry(long row, long col) {
        double numOfRowsPerInstance = (double) distributedMatrixType.rows() / distributedMatrixType.nodes().length;
        int partition = (int) (row / numOfRowsPerInstance);
        if(partition >= distributedMatrixType.nodes().length) {
            throw new IllegalStateException("The calculated partition id (row = " + row + ", distributedMatrixType.rows() = "
                    + distributedMatrixType.rows() +", numNodes = " + distributedMatrixType.nodes().length +") -> " + partition + " must not exceed numNodes.");
        }
        return distributedMatrixType.nodes()[partition];
    }

    @Override
    public long globalToLocalRow(long row) {
        long firstRow = matrixShape.rowOffset;
        long lastRow  = matrixShape.rowOffset + matrixShape.rows - 1;
        if(row < firstRow || row > lastRow) {
            throw new IllegalStateException(String.format("Can not translate row: %d. Not in current partitions row range: [%d, %d]", row, firstRow, lastRow));
        }
        return row - firstRow;
    }

    @Override
    public long globalToLocalColumn(long col) {
        return col;
    }

    @Override
    public long localToGlobalRow(long row) {
        if(row > matrixShape.rows - 1) {
            throw new IllegalStateException(String.format("Can not translate row: %d. Not in allowed range: [%d, %d]", row, 0, matrixShape.rows - 1));
        }
        return row + matrixShape.rowOffset;
    }

    @Override public long localToGlobalColumn(long col) { return col; }

    @Override public int getNumRowPartitions() { return distributedMatrixType.nodes().length; }

    @Override public int getNumColPartitions() { return 1; }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected MatrixPartitionShape computeMatrixPartitionShape() {
        if (distributedMatrixType.nodes() == null)
            return null;
        
        double rowsPerNode = (double) distributedMatrixType.globalRows() / distributedMatrixType.nodes().length;
        long rowOffset = (int) Math.ceil(rowsPerNode * distributedMatrixType.nodeId());
        long numRows = (int) (Math.ceil(rowsPerNode * (distributedMatrixType.nodeId() + 1)) - rowOffset);
        return new MatrixPartitionShape(numRows, distributedMatrixType.globalCols(), rowOffset, 0);
    }
}
