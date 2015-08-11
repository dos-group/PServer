package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;

public final class SlotContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final JobContext jobContext;

    public final int slotID;

    public final JobExecutable jobInvokeable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SlotContext(final JobContext jobContext,
                       final int slotID,
                       final JobExecutable jobInvokeable) {

        this.jobContext     = Preconditions.checkNotNull(jobContext);
        this.slotID         = Preconditions.checkNotNull(slotID);
        this.jobInvokeable  = Preconditions.checkNotNull(jobInvokeable);
    }
}
