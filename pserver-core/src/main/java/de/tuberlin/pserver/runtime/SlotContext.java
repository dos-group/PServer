package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.ControlFlow;
import de.tuberlin.pserver.dsl.dataflow.DataFlow;

public final class SlotContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final RuntimeContext runtimeContext;

    public final MLProgramContext programContext;

    public final int slotID;

    public final MLProgram programInvokeable;

    // ---------------------------------------------------

    public ControlFlow CF;

    public DataFlow DF;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SlotContext(final RuntimeContext runtimeContext,
                       final MLProgramContext programContext,
                       final int slotID,
                       final MLProgram programInvokeable) {

        this.runtimeContext     = Preconditions.checkNotNull(runtimeContext);
        this.programContext     = Preconditions.checkNotNull(programContext);
        this.slotID             = slotID;
        this.programInvokeable  = Preconditions.checkNotNull(programInvokeable);
        this.CF                 = new ControlFlow(this);
        this.DF                 = new DataFlow(this);
    }

    // ---------------------------------------------------
    // Public Method.
    // ---------------------------------------------------

    @Override
    public String toString() { return "[" + runtimeContext.nodeID + "|" + slotID + "]"; }

    //public SlotGroup getActiveSlotGroup() { return runtimeContext.executionManager.getActiveSlotGroup(); }

    public boolean node(final int fromNodeID, final int toNodeID) {
        return runtimeContext.nodeID >= fromNodeID && runtimeContext.nodeID <= toNodeID;
    }

    public boolean node(final int nodeID) { return node(nodeID, nodeID); }
}
