package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.net.NetManager;

public final class PServerContext {

    public final int instanceID;

    public final PServerJobDescriptor job;

    public final DHT dht;

    public final NetManager netManager;

    public final DataManager dataManager;

    public PServerContext(final int instanceID,
                          final PServerJobDescriptor job,
                          final DHT dht,
                          final NetManager netManager,
                          final DataManager dataManager) {

        this.instanceID     = instanceID;
        this.job            = Preconditions.checkNotNull(job);
        this.dht            = Preconditions.checkNotNull(dht);
        this.netManager     = Preconditions.checkNotNull(netManager);
        this.dataManager    = Preconditions.checkNotNull(dataManager);
    }
}
