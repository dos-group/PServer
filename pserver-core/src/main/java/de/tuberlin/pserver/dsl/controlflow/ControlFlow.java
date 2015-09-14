package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.controlflow.unit.SlotParallelUnit;
import de.tuberlin.pserver.runtime.SlotContext;

public final class ControlFlow {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private SlotContext slotContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ControlFlow(final SlotContext slotContext) {
        this.slotContext = Preconditions.checkNotNull(slotContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int numNodes() { return slotContext.programContext.nodeDOP; }

    public int numSlots() { return slotContext.programContext.perNodeDOP; }

    // ---------------------------------------------------

    public Loop loop() { return new Loop(slotContext); }

    // ---------------------------------------------------

    public SlotParallelUnit parUnit() {
        return new SlotParallelUnit(slotContext).allSlots();
    }

    public SlotParallelUnit parUnit(final int nodeID) {
        return new SlotParallelUnit(slotContext).slot(nodeID);
    }

    public SlotParallelUnit parUnit(final int fromSlotID, final int toSlotID) {
        return new SlotParallelUnit(slotContext).slot(fromSlotID, toSlotID);
    }

    public SlotParallelUnit serial() { return parUnit(0); }

    // ---------------------------------------------------

    public void syncSlots() { slotContext.runtimeContext.executionManager.localSync(slotContext); }
}
