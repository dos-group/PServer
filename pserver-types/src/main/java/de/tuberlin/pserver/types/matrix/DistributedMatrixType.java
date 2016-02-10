package de.tuberlin.pserver.types.matrix;


import de.tuberlin.pserver.types.DistributedType;
import de.tuberlin.pserver.types.matrix.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.matrix.partitioner.PartitionShape;
import de.tuberlin.pserver.types.matrix.partitioner.PartitionType;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class DistributedMatrixType implements DistributedType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int nodeID;

    public final int[] nodes;

    transient private Object owner;

    transient private final Lock lock;

    public final long globalRows;

    public final long globalCols;

    public final MatrixPartitioner partitioner;

    public final PartitionShape shape;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrixType(DistributedMatrixType m) {
        this(m.partitioner.getPartitionType(), m.nodeID, m.nodes, m.globalRows, m.globalCols);
    }

    public DistributedMatrixType(PartitionType type, int nodeID, int[] nodes, long globalRows, long globalCols) {
        this.nodes          = nodes;
        this.nodeID         = nodeID;
        this.globalRows     = globalRows;
        this.globalCols     = globalCols;
        this.partitioner    = MatrixPartitioner.createPartitioner(type, nodeID, nodes, globalRows, globalCols);
        this.shape          = partitioner.getPartitionShape();
        this.lock           = new ReentrantLock(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setOwner(final Object owner) { this.owner = owner; }

    public Object owner() { return owner; }

    // ---------------------------------------------------

    abstract public void setArray(final Object data);

    abstract public Object toArray();

    // ---------------------------------------------------

    public void lock() { lock.lock(); }

    public void unlock() { lock.unlock(); }

    // ---------------------------------------------------

    public long rows() { return shape.rows; }

    public long cols() { return shape.cols; }

    public long globalRows() { return globalRows; }

    public long globalCols() { return globalCols; }

    // ---------------------------------------------------

    public long sizeOf() { return shape.rows * shape.cols * Float.BYTES; }

    public long globalSizeOf() { return globalRows * globalCols * Float.BYTES; }

    // ---------------------------------------------------

    public int nodeId() { return nodeID; }

    public int[] nodes() { return nodes; }

    public MatrixPartitioner partitioner() { return partitioner; }

    public PartitionShape shape() { return shape; }
}
