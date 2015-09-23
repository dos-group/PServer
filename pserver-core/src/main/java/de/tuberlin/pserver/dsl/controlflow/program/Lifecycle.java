package de.tuberlin.pserver.dsl.controlflow.program;

import de.tuberlin.pserver.dsl.controlflow.base.Body;
import de.tuberlin.pserver.dsl.controlflow.base.CFStatement;
import de.tuberlin.pserver.runtime.SlotContext;

public final class Lifecycle extends CFStatement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public Body preProcessPhase;

    public Body processPhase;

    public Body postProcessPhase;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Lifecycle(final SlotContext slotContext) {
        super(slotContext);
    }

    // ---------------------------------------------------
    // Life-Cycle.
    // ---------------------------------------------------

    public Lifecycle preProcess(final Body b) { preProcessPhase = b; return this; }

    public Lifecycle process(final Body b) { processPhase = b; return this; }

    public Lifecycle postProcess(final Body b) { postProcessPhase = b; return this; }
}
