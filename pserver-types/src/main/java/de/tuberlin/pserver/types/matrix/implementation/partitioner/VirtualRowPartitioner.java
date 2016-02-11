package de.tuberlin.pserver.types.matrix.implementation.partitioner;

import de.tuberlin.pserver.types.matrix.implementation.f32.entries.Entry32F;

/**
 * Partitions a matrix into one partition of the same size as the matrix.
 * Used if no partitioning is applied.
 */
public class VirtualRowPartitioner extends MatrixPartitioner {

    private final MatrixPartitionShape shape;

    public VirtualRowPartitioner(PartitionType partitionType, long rows, long cols, int nodeId, int[] atNodes) {
        super(partitionType, rows, cols, nodeId, atNodes);
        shape = new MatrixPartitionShape(rows, cols, 0, 0);
    }

    @Override public int getPartitionOfEntry(long row, long col) {
        return nodeId;
    }

    @Override public int getPartitionOfEntry(Entry32F entry) {
        return nodeId;
    }

    @Override public MatrixPartitionShape getPartitionShape() {
        return shape;
    }

    @Override public long translateGlobalToLocalRow(long row) { return row; }

    @Override public long translateGlobalToLocalCol(long col) { return col; }

    @Override public long translateLocalToGlobalRow(long row) { return row; }

    @Override public long translateLocalToGlobalCol(long col) { return col; }

    @Override public int getNumRowPartitions() { return 1; }

    @Override public int getNumColPartitions() { return 1; }
}
