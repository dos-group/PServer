package de.tuberlin.pserver.dsl.controlflow.program;

import de.tuberlin.pserver.dsl.controlflow.base.Body;
import de.tuberlin.pserver.dsl.controlflow.base.CFStatement;
import de.tuberlin.pserver.runtime.SlotContext;

public final class Program extends CFStatement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public Body initPhase;

    public Body preProcessPhase;

    public Body processPhase;

    public Body postProcessPhase;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Program(final SlotContext slotContext) {
        super(slotContext);
    }

    // ---------------------------------------------------
    // Life-Cycle.
    // ---------------------------------------------------

    public Program initialize(final Body b) { initPhase = b; return this; }

    public Program preProcess(final Body b) { preProcessPhase = b; return this; }

    public Program process(final Body b) { processPhase = b; return this; }

    public Program postProcess(final Body b) { postProcessPhase = b; return this; }
}
