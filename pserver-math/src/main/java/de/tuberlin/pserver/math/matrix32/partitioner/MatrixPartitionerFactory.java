package de.tuberlin.pserver.math.matrix32.partitioner;

public final class MatrixPartitionerFactory {

    public static MatrixPartitioner createPartitioner(PartitionerType type, int nodeID, int[] nodes, long globalRows, long globalCols) {
        switch (type) {
            case NO_PARTITIONER:
                return new NoPartitioner(type, globalRows, globalCols, nodeID, nodes);
            case ROW_PARTITIONER:
                return new RowPartitioner(type, globalRows, globalCols, nodeID, nodes);
            case ROW_VIRTUAL_PARTITIONER:
                return new VirtualRowPartitioner(type, globalRows, globalCols, nodeID, nodes);
        }
        throw new IllegalStateException();
    }
}
