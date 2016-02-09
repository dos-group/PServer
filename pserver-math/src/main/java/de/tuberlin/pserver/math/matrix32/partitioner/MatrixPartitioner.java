package de.tuberlin.pserver.math.matrix32.partitioner;

import de.tuberlin.pserver.math.matrix32.entries.Entry32;

import java.util.stream.IntStream;

public abstract class MatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final long  rows;

    protected final long  cols;

    protected final int   nodeId;

    protected final int[] atNodes;

    protected final PartitionerType partitionerType;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /*/public MatrixPartitioner() { this(-1, -1, -1, null); }
    public MatrixPartitioner(long rows, long cols, int nodeId, int numNodes) {
        this(rows, cols, nodeId, IntStream.iterate(0, x -> x + 1).limit(numNodes).toArray());
    }*/
    public MatrixPartitioner(PartitionerType partitionerType, long rows, long cols, int nodeId, int[] atNodes) {
        this.partitionerType = partitionerType;
        this.rows    = rows;
        this.cols    = cols;
        this.nodeId  = nodeId;
        this.atNodes = atNodes;
    }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public PartitionerType getPartitionerType() { return partitionerType; }

    public abstract int getPartitionOfEntry(long row, long col);

    public abstract int getPartitionOfEntry(Entry32 entry);

    public abstract PartitionShape getPartitionShape();

    public abstract long translateGlobalToLocalRow(long row);

    public abstract long translateGlobalToLocalCol(long col);

    public abstract long translateLocalToGlobalRow(long row);

    public abstract long translateLocalToGlobalCol(long col);

    public abstract int getNumRowPartitions();

    public abstract int getNumColPartitions();

    public int getNodeId() { return nodeId; }

    public int[] getNodes() { return atNodes; }
}
