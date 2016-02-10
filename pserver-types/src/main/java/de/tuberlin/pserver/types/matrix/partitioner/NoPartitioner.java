package de.tuberlin.pserver.types.matrix.partitioner;

import de.tuberlin.pserver.types.matrix.f32.entries.Entry32F;

public class NoPartitioner extends MatrixPartitioner {

    private final PartitionShape shape;

    public NoPartitioner(PartitionType partitionType, long rows, long cols, int nodeId, int[] atNodes) {
        super(partitionType, rows, cols, nodeId, atNodes);
        shape = new PartitionShape(rows, cols, 0, 0);
    }

    @Override public int getPartitionOfEntry(long row, long col) { return -1; }

    @Override public int getPartitionOfEntry(Entry32F entry) { return -1; }

    @Override public PartitionShape getPartitionShape() { return shape; }

    @Override public long translateGlobalToLocalRow(long row) { return row; }

    @Override public long translateGlobalToLocalCol(long col) { return col; }

    @Override public long translateLocalToGlobalRow(long row) { return row; }

    @Override public long translateLocalToGlobalCol(long col) { return col; }

    @Override public int getNumRowPartitions() { return 1; }

    @Override public int getNumColPartitions() { return 1; }
}
