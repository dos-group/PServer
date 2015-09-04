package de.tuberlin.pserver.dsl.controlflow.selection;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.SharedVar;
import de.tuberlin.pserver.dsl.controlflow.Body;
import de.tuberlin.pserver.dsl.controlflow.CFStatement;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.SlotContext;

import java.util.concurrent.CyclicBarrier;

public final class Selection extends CFStatement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ExecutionManager executionManager;

    private int fromNodeID;

    private int toNodeID;

    private int fromSlotID;

    private int toSlotID;

    private CyclicBarrier slotGroupBarrier;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Selection(final SlotContext ic) {
        super(ic);
        executionManager = ic.programContext.runtimeContext.executionManager;
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

    public Selection slot(final int fromSlotID, final int toSlotID) {
        this.fromSlotID = fromSlotID;
        this.toSlotID = toSlotID;
        return this;
    }

    public Selection slot(final int slotID) { return slot(slotID, slotID); }

    public Selection allSlots() { return slot(0, slotContext.programContext.perNodeDOP - 1); }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public Selection sync() throws Exception {

        if (slotGroupBarrier != null)
            slotGroupBarrier.await();

        return this;
    }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public void exe(final Body body) throws Exception {
        Preconditions.checkNotNull(body);



        if (inNodeRange() && inInstanceRange()) {

            final ExecutionManager.SlotGroupAllocation sa = executionManager.allocSlots(fromSlotID, toSlotID);

            final int numActiveSlots = (toSlotID - fromSlotID) + 1;

            if (numActiveSlots > 1) {

                final SharedVar<CyclicBarrier> sgb = new SharedVar<>(slotContext, new CyclicBarrier(numActiveSlots));

                slotGroupBarrier = sgb.get();

                sgb.done();
            }

            sync();

                enter();

                    long duration = System.currentTimeMillis();

                        body.body();

                    duration = System.currentTimeMillis() - duration;

                    setProfilingData(new ProfilingData(duration));

                leave();

            sync();

            executionManager.releaseSlots(sa);
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
        return slotContext.slotID >= fromSlotID
                && slotContext.slotID <= toSlotID;
    }
}
