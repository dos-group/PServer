package de.tuberlin.pserver.math.matrix32;


import de.tuberlin.pserver.math.matrix32.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.math.matrix32.partitioner.MatrixPartitionerFactory;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionShape;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionerType;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class Matrix32MetaData {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int nodeID;

    public final int[] nodes;

    public final MatrixPartitioner partitioner;

    public final PartitionShape shape;

    public final long globalRows;

    public final long globalCols;

    transient private final Lock lock;

    transient private Object owner;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Matrix32MetaData(Matrix32MetaData m) {
        this(m.partitioner.getPartitionerType(), m.nodeID, m.nodes, m.globalRows, m.globalCols);
    }

    public Matrix32MetaData(PartitionerType type, int nodeID, int[] nodes, long globalRows, long globalCols) {
        this.nodes = nodes;
        this.nodeID = nodeID;
        this.globalRows = globalRows;
        this.globalCols = globalCols;
        this.partitioner = MatrixPartitionerFactory.createPartitioner(type, nodeID, nodes, globalRows, globalCols);
        this.shape = partitioner.getPartitionShape();
        this.lock = new ReentrantLock(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setOwner(final Object owner) { this.owner = owner; }

    public Object getOwner() { return owner; }

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
}
