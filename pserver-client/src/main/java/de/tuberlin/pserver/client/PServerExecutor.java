package de.tuberlin.pserver.client;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.node.PServerMain;
import de.tuberlin.pserver.runtime.MLProgram;
import org.apache.log4j.ConsoleAppender;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public enum PServerExecutor {

    LOCAL(true),

    DISTRIBUTED(false);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final boolean isLocal;

    private ClusterSimulator simulator;

    private PServerClient client;

    private UUID currentJob = null;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private PServerExecutor(final boolean isLocal) {
        org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender());
        this.isLocal = isLocal;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public PServerExecutor run(final Class<? extends MLProgram> jobClass) {
        if (isLocal) {
            simulator = new ClusterSimulator(
                    IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION),
                    PServerMain.class
            );
        } else
            simulator = null;

        client = PServerClientFactory.createPServerClient();
        currentJob = client.execute(Preconditions.checkNotNull(jobClass));
        return this;
    }

    public PServerExecutor results(final List<List<Serializable>> results) {
        Preconditions.checkState(currentJob != null);
        for (int i = 0; i < client.getNumberOfWorkers(); ++i)
            results.add(client.getResultsFromWorker(currentJob, i));
        return this;
    }

    public void done() {
        client.deactivate();
        if (simulator != null) {
            simulator.deactivate();
        }
    }
}
