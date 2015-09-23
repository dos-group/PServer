package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.loop.Loop;
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

    // ---------------------------------------------------

    public Loop loop() { return new Loop(slotContext); }
}
