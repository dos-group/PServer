package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;

public final class SlotContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final MLProgramContext programContext;

    public final int slotID;

    public final MLProgram programInvokeable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SlotContext(final MLProgramContext programContext,
                       final int slotID,
                       final MLProgram programInvokeable) {

        this.programContext     = Preconditions.checkNotNull(programContext);
        this.slotID             = Preconditions.checkNotNull(slotID);
        this.programInvokeable  = Preconditions.checkNotNull(programInvokeable);
    }
}
