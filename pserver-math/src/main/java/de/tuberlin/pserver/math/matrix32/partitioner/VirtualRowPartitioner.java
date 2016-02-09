package de.tuberlin.pserver.math.matrix32.partitioner;

import de.tuberlin.pserver.math.matrix32.entries.Entry32;

/**
 * Partitions a matrix into one partition of the same size as the matrix.
 * Used if no partitioning is applied.
 */
public class VirtualRowPartitioner extends MatrixPartitioner {

    private final PartitionShape shape;

    public VirtualRowPartitioner(PartitionerType partitionerType, long rows, long cols, int nodeId, int[] atNodes) {
        super(partitionerType, rows, cols, nodeId, atNodes);
        shape = new PartitionShape(rows, cols, 0, 0);
    }

    @Override public int getPartitionOfEntry(long row, long col) {
        return nodeId;
    }

    @Override public int getPartitionOfEntry(Entry32 entry) {
        return nodeId;
    }

    @Override public PartitionShape getPartitionShape() {
        return shape;
    }

    @Override public long translateGlobalToLocalRow(long row) { return row; }

    @Override public long translateGlobalToLocalCol(long col) { return col; }

    @Override public long translateLocalToGlobalRow(long row) { return row; }

    @Override public long translateLocalToGlobalCol(long col) { return col; }

    @Override public int getNumRowPartitions() { return 1; }

    @Override public int getNumColPartitions() { return 1; }
}
