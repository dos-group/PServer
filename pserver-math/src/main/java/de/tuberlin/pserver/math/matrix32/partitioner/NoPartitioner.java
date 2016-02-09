package de.tuberlin.pserver.math.matrix32.partitioner;

import de.tuberlin.pserver.math.matrix32.entries.Entry32;

public class NoPartitioner extends MatrixPartitioner {

    private final PartitionShape shape;

    public NoPartitioner(PartitionerType partitionerType, long rows, long cols, int nodeId, int[] atNodes) {
        super(partitionerType, rows, cols, nodeId, atNodes);
        shape = new PartitionShape(rows, cols, 0, 0);
    }

    @Override public int getPartitionOfEntry(long row, long col) { return -1; }

    @Override public int getPartitionOfEntry(Entry32 entry) { return -1; }

    @Override public PartitionShape getPartitionShape() { return shape; }

    @Override public long translateGlobalToLocalRow(long row) { return row; }

    @Override public long translateGlobalToLocalCol(long col) { return col; }

    @Override public long translateLocalToGlobalRow(long row) { return row; }

    @Override public long translateLocalToGlobalCol(long col) { return col; }

    @Override public int getNumRowPartitions() { return 1; }

    @Override public int getNumColPartitions() { return 1; }
}
