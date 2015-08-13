package de.tuberlin.pserver.dsl.controlflow.selection;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.Body;
import de.tuberlin.pserver.dsl.controlflow.CFStatement;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.SlotContext;

public final class Selection extends CFStatement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ExecutionManager exeManager;

    private int fromNodeID;

    private int toNodeID;

    private int fromInstanceID;

    private int toInstanceID;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Selection(final SlotContext ic) {
        super(ic);

        exeManager = ic.programContext.runtimeContext.executionManager;

        allNodes();
        allSlots();
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

    public Selection allNodes() { return node(0, slotContext.programContext.nodeDOP - 1); }

    // ---------------------------------------------------
    // Instance Selection.
    // ---------------------------------------------------

    public Selection slot(final int fromInstanceID, final int toInstanceID) {
        this.fromInstanceID = fromInstanceID;
        this.toInstanceID = toInstanceID;
        return this;
    }

    public Selection slot(final int instanceID) { return slot(instanceID, instanceID); }

    public Selection allSlots() { return slot(0, slotContext.programContext.perNodeDOP - 1); }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Selection sync() { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public void exe(final Body body) throws Exception {
        Preconditions.checkNotNull(body);
        if (inNodeRange() && inInstanceRange()) {

            final ExecutionManager.SlotAllocation sa = exeManager.allocSlots(fromInstanceID, toInstanceID);

            enter();

            long duration = System.currentTimeMillis();

            body.body();

            duration = System.currentTimeMillis() - duration;

            setProfilingData(new ProfilingData(duration));

            leave();

            exeManager.releaseSlots(sa);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private boolean inNodeRange() {
        return slotContext.programContext.runtimeContext.nodeID >= fromNodeID
                && slotContext.programContext.runtimeContext.nodeID <= toNodeID;
    }

    private boolean inInstanceRange() {
        return slotContext.slotID >= fromInstanceID
                && slotContext.slotID <= toInstanceID;
    }
}
