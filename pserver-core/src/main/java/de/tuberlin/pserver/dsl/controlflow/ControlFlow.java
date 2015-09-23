package de.tuberlin.pserver.dsl.controlflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.loop.Loop;
import de.tuberlin.pserver.runtime.ProgramContext;

public final class ControlFlow {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private ProgramContext programContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ControlFlow(final ProgramContext programContext) {
        this.programContext = Preconditions.checkNotNull(programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int numNodes() { return programContext.nodeDOP; }

    // ---------------------------------------------------

    public Loop loop() { return new Loop(programContext); }
}
