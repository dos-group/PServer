package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;

public final class InstanceContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final JobContext jobContext;

    public final int instanceID;

    public final PServerJob jobInvokeable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InstanceContext(final JobContext jobContext,
                           final int instanceID,
                           final PServerJob jobInvokeable) {

        this.jobContext     = Preconditions.checkNotNull(jobContext);
        this.instanceID     = Preconditions.checkNotNull(instanceID);
        this.jobInvokeable  = Preconditions.checkNotNull(jobInvokeable);
    }
}
