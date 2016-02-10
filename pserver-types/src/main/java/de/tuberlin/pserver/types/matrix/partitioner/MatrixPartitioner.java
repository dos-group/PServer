package de.tuberlin.pserver.types.matrix.partitioner;

import de.tuberlin.pserver.types.matrix.f32.entries.Entry32F;

public abstract class MatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final long  rows;

    protected final long  cols;

    protected final int   nodeId;

    protected final int[] atNodes;

    protected final PartitionType partitionType;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixPartitioner(PartitionType partitionType, long rows, long cols, int nodeId, int[] atNodes) {
        this.partitionType = partitionType;
        this.rows    = rows;
        this.cols    = cols;
        this.nodeId  = nodeId;
        this.atNodes = atNodes;
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    public static MatrixPartitioner createPartitioner(PartitionType type, int nodeID, int[] nodes, long globalRows, long globalCols) {
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

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public PartitionType getPartitionType() { return partitionType; }

    public abstract int getPartitionOfEntry(long row, long col);

    public abstract int getPartitionOfEntry(Entry32F entry);

    public abstract MatrixPartitionShape getPartitionShape();

    public abstract long translateGlobalToLocalRow(long row);

    public abstract long translateGlobalToLocalCol(long col);

    public abstract long translateLocalToGlobalRow(long row);

    public abstract long translateLocalToGlobalCol(long col);

    public abstract int getNumRowPartitions();

    public abstract int getNumColPartitions();

    public int getNodeId() { return nodeId; }

    public int[] getNodes() { return atNodes; }
}
