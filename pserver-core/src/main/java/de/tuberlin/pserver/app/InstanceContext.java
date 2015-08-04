package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.utils.ResettableCountDownLatch;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public final class InstanceContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final JobContext jobContext;

    public final int threadID;

    public final PServerJob jobInvokeable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InstanceContext(final JobContext jobContext,
                           final int threadID,
                           final PServerJob jobInvokeable) {

        this.jobContext     = Preconditions.checkNotNull(jobContext);
        this.threadID       = Preconditions.checkNotNull(threadID);
        this.jobInvokeable  = Preconditions.checkNotNull(jobInvokeable);
    }
}
