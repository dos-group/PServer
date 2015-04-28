package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;

import java.util.UUID;

public final class PServerContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final MachineDescriptor clientMachine;

    public final UUID jobUID;

    public final String className;

    public final String simpleClassName;

    public final int perNodeParallelism;

    public final int instanceID;

    public final int threadID;

    public final NetManager netManager;

    public final DHT dht;

    public final DataManager dataManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerContext(final MachineDescriptor clientMachine,
                          final UUID jobUID,
                          final String className,
                          final String simpleClassName,
                          final int perNodeParallelism,
                          final int instanceID,
                          final int threadID,
                          final NetManager netManager,
                          final DHT dht,
                          final DataManager dataManager) {

        this.clientMachine      = Preconditions.checkNotNull(clientMachine);
        this.jobUID             = Preconditions.checkNotNull(jobUID);
        this.className          = Preconditions.checkNotNull(className);
        this.simpleClassName    = Preconditions.checkNotNull(simpleClassName);
        this.perNodeParallelism = perNodeParallelism;
        this.instanceID         = instanceID;
        this.threadID           = threadID;
        this.dht                = Preconditions.checkNotNull(dht);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.dataManager        = Preconditions.checkNotNull(dataManager);
    }
}
