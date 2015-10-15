package de.tuberlin.pserver.dsl.state;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.ProgramContext;

public final class StateMng {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static ProgramContext programContext;

    private static StateBuilder stateBuilder;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public static void setProgramContext(final ProgramContext programContext) {
        StateMng.programContext = Preconditions.checkNotNull(programContext);
        StateMng.stateBuilder   = new StateBuilder(StateMng.programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static StateBuilder getStateBuilder() {
        stateBuilder.clear();
        return stateBuilder;
    }
}
