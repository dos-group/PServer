package de.tuberlin.pserver.runtime.state.matrix.partitioner;

import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.runtime.state.matrix.entries.Entry;

public class NoPartitioner extends MatrixPartitioner {

    private final PartitionShape shape;

    public NoPartitioner(long rows, long cols, int nodeId, int[] atNodes) {
        super(rows, cols, nodeId, atNodes);
        shape = new PartitionShape(rows, cols, 0, 0);
    }

    @Override
    public int getPartitionOfEntry(long row, long col) {
        return -1;
    }

    @Override
    public int getPartitionOfEntry(Entry entry) {
        return -1;
    }

    @Override
    public PartitionShape getPartitionShape() {
        return shape;
    }

    @Override
    public long translateGlobalToLocalRow(long row) throws IllegalArgumentException {
        return row;
    }

    @Override
    public long translateGlobalToLocalCol(long col) throws IllegalArgumentException {
        return col;
    }

    @Override
    public long translateLocalToGlobalRow(long row) throws IllegalArgumentException {
        return row;
    }

    @Override
    public long translateLocalToGlobalCol(long col) throws IllegalArgumentException {
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
    public MatrixPartitioner ofNode(int nodeId) {
        return new VirtualRowPartitioner(rows, cols, nodeId, new int[] {nodeId});
    }
}