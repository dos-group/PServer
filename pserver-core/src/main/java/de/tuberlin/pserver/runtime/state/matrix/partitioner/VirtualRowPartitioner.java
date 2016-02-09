package de.tuberlin.pserver.runtime.state.matrix.partitioner;

import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.runtime.filesystem.records.Entry;

/**
 * Partitions a matrix into one partition of the same size as the matrix.
 * Used if no partitioning is applied.
 */
public class VirtualRowPartitioner extends MatrixPartitioner {

    private final PartitionShape shape;

    public VirtualRowPartitioner() { this(-1, -1, -1, null); }
    public VirtualRowPartitioner(long rows, long cols, int nodeId, int[] atNodes) {
        super(rows, cols, nodeId, atNodes);
        shape = new PartitionShape(rows, cols, 0, 0);
    }

    @Override
    public int getPartitionOfEntry(long row, long col) {
        return nodeId;
    }

    @Override
    public int getPartitionOfEntry(Entry entry) {
        return nodeId;
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
