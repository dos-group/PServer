package de.tuberlin.pserver.types.metadata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractDistributedType implements DistributedType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int nodeID;

    public final int[] nodes;

    public final DistScheme distScheme;

    transient private Object owner;

    transient private final Lock lock;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractDistributedType(AbstractDistributedType m) {
        this(m.nodeID, m.nodes, m.distScheme);
    }

    public AbstractDistributedType(int nodeID, int[] nodes, DistScheme distScheme) {
        this.nodes = nodes;
        this.nodeID = nodeID;
        this.distScheme = distScheme;
        this.lock = new ReentrantLock(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int nodeId() { return nodeID; }

    public int[] nodes() { return nodes; }

    // ---------------------------------------------------

    public DistScheme distributionScheme() { return distScheme; }

    // ---------------------------------------------------

    public void owner(final Object owner) { this.owner = owner; }

    public Object owner() { return owner; }

    // ---------------------------------------------------

    public void lock() { lock.lock(); }

    public void unlock() { lock.unlock(); }
}
