package de.tuberlin.pserver.dsl.state;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;

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
