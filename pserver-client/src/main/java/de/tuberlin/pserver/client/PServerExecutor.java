package de.tuberlin.pserver.client;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.node.PServerMain;
import org.apache.log4j.ConsoleAppender;

public enum PServerExecutor {
    LOCAL(true),

    DISTRIBUTED(false);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ClusterSimulator simulator;

    private final PServerClient client;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private PServerExecutor(final boolean isLocal) {

        org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender());

        if (isLocal) {
            simulator = new ClusterSimulator(
                    IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION),
                    PServerMain.class
            );
        } else
            simulator = null;

        client = PServerClientFactory.createPServerClient();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public PServerExecutor run(final Class<? extends PServerJob> jobClass) {
        Preconditions.checkState(client != null);
        client.execute(Preconditions.checkNotNull(jobClass));
        return this;
    }

    public void done() {
        if (client != null)
            client.deactivate();
        else
            throw new IllegalStateException();
        if (simulator != null)
            simulator.deactivate();
    }
}
