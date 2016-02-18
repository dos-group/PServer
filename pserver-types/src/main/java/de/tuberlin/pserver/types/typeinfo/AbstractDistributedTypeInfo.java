package de.tuberlin.pserver.types.typeinfo;

import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;
import de.tuberlin.pserver.types.typeinfo.properties.InputDescriptor;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractDistributedTypeInfo implements DistributedTypeInfo {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int nodeID;

    public final int[] nodes;

    public final String name;

    public final Class<?> type;

    public boolean globalAccess = false;

    public final DistScheme distScheme;

    transient private Object owner;

    transient private final Lock lock;

    private InputDescriptor inputDescriptor;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractDistributedTypeInfo() {
        this(-1, null, null, null, null);
    }

    public AbstractDistributedTypeInfo(AbstractDistributedTypeInfo m) {
        this(m.nodeID, m.nodes, m.type(), m.name(), m.distScheme);
    }

    public AbstractDistributedTypeInfo(int nodeID, int[] nodes, Class<?> type, String name, DistScheme distScheme) {
        this.nodes      = nodes;
        this.nodeID     = nodeID;
        this.type       = type;
        this.name       = name;
        this.distScheme = distScheme;
        this.lock       = new ReentrantLock(true);
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

    // ---------------------------------------------------

    public String name() { return name; }

    public Class<?> type() { return type; }

    public void setGlobalAccess() { globalAccess = true; }

    public boolean hasGlobalAccess() { return globalAccess; }

    // ---------------------------------------------------

    public void input(InputDescriptor id) { inputDescriptor = id; }

    public InputDescriptor input() { return inputDescriptor; }
}
