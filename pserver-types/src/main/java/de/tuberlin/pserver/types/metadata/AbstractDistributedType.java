package de.tuberlin.pserver.types.metadata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractDistributedType implements DistributedType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int nodeID;

    public final int[] nodes;

    public final DistributionScheme distributionScheme;

    transient private Object owner;

    transient private final Lock lock;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractDistributedType(AbstractDistributedType m) {
        this(m.nodeID, m.nodes, m.distributionScheme);
    }

    public AbstractDistributedType(int nodeID, int[] nodes, DistributionScheme distributionScheme) {
        this.nodes = nodes;
        this.nodeID = nodeID;
        this.distributionScheme = distributionScheme;
        this.lock = new ReentrantLock(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int nodeId() { return nodeID; }

    public int[] nodes() { return nodes; }

    // ---------------------------------------------------

    public DistributionScheme distributionScheme() { return distributionScheme; }

    // ---------------------------------------------------

    public void owner(final Object owner) { this.owner = owner; }

    public Object owner() { return owner; }

    // ---------------------------------------------------

    public void lock() { lock.lock(); }

    public void unlock() { lock.unlock(); }
}
