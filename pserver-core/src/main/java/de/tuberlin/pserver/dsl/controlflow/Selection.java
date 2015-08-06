package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;


public final class Selection {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final InstanceContext instanceContext;

    private int fromNodeID;

    private int toNodeID;

    private int fromInstanceID;

    private int toInstanceID;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Selection(final InstanceContext instanceContext) {
        this.instanceContext = Preconditions.checkNotNull(instanceContext);
        allNodes();
        allInstances();
    }

    // ---------------------------------------------------
    // Node Selection.
    // ---------------------------------------------------

    public Selection node(final int fromNodeID, final int toNodeID) {
        this.fromNodeID = fromNodeID;
        this.toNodeID = toNodeID;
        return this;
    }

    public Selection node(final int nodeID) { return node(nodeID, nodeID); }

    public Selection allNodes() { return node(0, instanceContext.jobContext.numOfNodes - 1); }

    // ---------------------------------------------------
    // Instance Selection.
    // ---------------------------------------------------

    public Selection instance(final int fromInstanceID, final int toInstanceID) {
        this.fromInstanceID = fromInstanceID;
        this.toInstanceID = toInstanceID;
        return this;
    }

    public Selection instance(final int instanceID) { return instance(instanceID, instanceID); }

    public Selection allInstances() { return instance(0, instanceContext.jobContext.numOfInstances - 1); }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Selection sync() { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public void execute(final Body body) {
        Preconditions.checkNotNull(body);
        if (inNodeRange() && inInstanceRange())
            body.body();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private boolean inNodeRange() {
        return instanceContext.jobContext.nodeID >= fromNodeID
                && instanceContext.jobContext.nodeID <= toNodeID;
    }

    private boolean inInstanceRange() {
        return instanceContext.instanceID >= fromInstanceID
                && instanceContext.instanceID <= toInstanceID;
    }
}
