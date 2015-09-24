package de.tuberlin.pserver.dsl.unit;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.LoopBody;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.LoopTermination;
import de.tuberlin.pserver.runtime.ProgramContext;

public final class UnitMng {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static ProgramContext programContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public static void setProgramContext(final ProgramContext programContext) {
        UnitMng.programContext = Preconditions.checkNotNull(programContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static Loop loop() { return new Loop(programContext); }


    public static Loop loop(final long n, final int mode, final LoopBody body) throws Exception {

        return new Loop(programContext).sync(mode).exe(n, body);
    }

    public static Loop loop(final long n, final LoopBody body) throws Exception {

        return new Loop(programContext).exe(n, body);
    }

    public static Loop loop(final LoopTermination t, final LoopBody body) throws Exception {

        return new Loop(programContext).exe(t, body);
    }
}
