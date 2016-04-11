package de.tuberlin.pserver.dsl.unit.controlflow.lifecycle;

import de.tuberlin.pserver.dsl.unit.controlflow.base.Body;
import de.tuberlin.pserver.dsl.unit.controlflow.base.CFStatement;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

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

    public Lifecycle(final ProgramContext programContext) {
        super(programContext);
    }

    // ---------------------------------------------------
    // Life-Cycle.
    // ---------------------------------------------------

    public Lifecycle preProcess(final Body b) { preProcessPhase = b; return this; }

    public Lifecycle process(final Body b) { processPhase = b; return this; }

    public Lifecycle postProcess(final Body b) { postProcessPhase = b; return this; }
}
