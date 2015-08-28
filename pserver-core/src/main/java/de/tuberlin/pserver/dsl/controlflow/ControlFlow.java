package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.iteration.Iteration;
import de.tuberlin.pserver.dsl.controlflow.selection.Selection;
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

    public int numInstances() { return slotContext.programContext.perNodeDOP; }

    public Iteration iterate() { return new Iteration(slotContext); }

    public Selection select() {return new Selection(slotContext); }

    public void syncSlots() { slotContext.programContext.runtimeContext.executionManager.localSync(slotContext); }
}
