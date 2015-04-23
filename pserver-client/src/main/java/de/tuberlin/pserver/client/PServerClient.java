package de.tuberlin.pserver.client;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.PServerJobDescriptor;
import de.tuberlin.pserver.app.UserCodeManager;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public final class PServerClient extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(PServerClient.class);

    private final IConfig config;

    private final MachineDescriptor machine;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final UserCodeManager userCodeManager;

    private final List<MachineDescriptor> pServerWorkers;

    private final Map<UUID, CountDownLatch> activeJobs;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerClient(final PServerClientFactory factory) {
        super(true, "PSERVER-CLIENT-THREAD");
        Preconditions.checkNotNull(factory);

        this.config          = factory.config;
        this.machine         = factory.machine;
        this.pServerWorkers  = factory.pServerWorkers;
        this.infraManager    = factory.infraManager;
        this.netManager      = factory.netManager;
        this.userCodeManager = factory.userCodeManager;
        this.activeJobs      = new HashMap<>();

        this.netManager.addEventListener(PServerJobDescriptor.PSERVER_FINISH_JOB_EVENT, new ClientFinishedJobEvent());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class ClientFinishedJobEvent implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final UUID jobUID = (UUID) e.getPayload();
            final CountDownLatch jobLatch = activeJobs.get(jobUID);
            if (jobLatch != null) {
                jobLatch.countDown();
            } else {
                throw new IllegalStateException();
            }
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IConfig getConfig() { return config; }

    public void execute(final Class<? extends PServerJob> jobClass) {
        Preconditions.checkNotNull(jobClass);

        final long start = System.nanoTime();

        final Triple<Class<?>, List<String>, byte[]> classData = userCodeManager.extractClass(jobClass);

        final PServerJobDescriptor jobDescriptor = new PServerJobDescriptor(
                machine,
                UUID.randomUUID(),
                classData.getLeft().getName(),
                classData.getLeft().getSimpleName(),
                classData.getMiddle(),
                classData.getRight()
        );

        final CountDownLatch jobLatch = new CountDownLatch(pServerWorkers.size());
        activeJobs.put(jobDescriptor.jobUID, jobLatch);

        LOG.info("Submit Job '" + jobDescriptor.simpleClassName + "' to worker nodes.");

        for (final MachineDescriptor md : pServerWorkers) {
            final NetEvents.NetEvent event = new NetEvents.NetEvent(PServerJobDescriptor.PSERVER_SUBMIT_JOB_EVENT);
            event.setPayload(jobDescriptor);
            netManager.sendEvent(md, event);
        }

        try {
            jobLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        LOG.info("Job '" + jobDescriptor.simpleClassName
                + "' [" + jobDescriptor.jobUID +"] finished in "
                + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms.");
    }

    @Override
    public void deactivate() {
        netManager.deactivate();
        infraManager.deactivate();
        super.deactivate();
    }
}
