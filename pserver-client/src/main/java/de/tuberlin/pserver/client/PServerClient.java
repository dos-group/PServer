package de.tuberlin.pserver.client;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.*;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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

    private final List<MachineDescriptor> workers;

    private final Map<UUID, CountDownLatch> activeJobs;

    private final Map<Pair<UUID,Integer>, List<Serializable>> jobResults;

    //private final Map<String, UUID> nameUIDMapping;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerClient(final PServerClientFactory factory) {
        super(true, "PSERVER-CLIENT-THREAD");
        Preconditions.checkNotNull(factory);

        this.config          = factory.config;
        this.machine         = factory.machine;
        this.workers         = factory.workers;
        this.infraManager    = factory.infraManager;
        this.netManager      = factory.netManager;
        this.userCodeManager = factory.userCodeManager;
        this.activeJobs      = new HashMap<>();
        this.jobResults      = new HashMap<>();
        //this.nameUIDMapping  = new HashMap<>();

        this.netManager.addEventListener(PServerJobFailureEvent.PSERVER_FAILURE_JOB_EVENT, new JobFailureEvent());
        this.netManager.addEventListener(PServerJobResultEvent.PSERVER_JOB_RESULT_EVENT, new JobResultEvent());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class JobFailureEvent implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final PServerJobFailureEvent jfe = (PServerJobFailureEvent)e;
            LOG.error(jfe.toString());
        }
    }

    private final class JobResultEvent implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final PServerJobResultEvent jre = (PServerJobResultEvent) e;
            jobResults.put(Pair.of(jre.jobUID, jre.instanceID), jre.resultObjects);
            final CountDownLatch jobLatch = activeJobs.get(jre.jobUID);
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

    public UUID execute(final Class<? extends PServerJob> jobClass) { return execute(jobClass, 1); }
    public UUID execute(final Class<? extends PServerJob> jobClass, final int perNodeParallelism) {
        Preconditions.checkNotNull(jobClass);
        Preconditions.checkArgument(perNodeParallelism >= 1);

        final long start = System.nanoTime();
        final Triple<Class<?>, List<String>, byte[]> classData = userCodeManager.extractClass(jobClass);
        final UUID jopUID = UUID.randomUUID();
        final PServerJobSubmissionEvent jobSubmission = new PServerJobSubmissionEvent(
                machine,
                jopUID,
                classData.getLeft().getName(),
                classData.getLeft().getSimpleName(),
                perNodeParallelism,
                classData.getMiddle(),
                classData.getRight()
        );

        final CountDownLatch jobLatch = new CountDownLatch(workers.size());
        activeJobs.put(jopUID, jobLatch);
        //nameUIDMapping.put(jobSubmission.simpleClassName, jopUID);

        LOG.info("Submit Job '" + jobSubmission.simpleClassName + "'.");
        workers.forEach(md -> netManager.sendEvent(md, jobSubmission));

        try {
            jobLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        LOG.info("Job '" + jobSubmission.simpleClassName
                + "' [" + jobSubmission.jobUID +"] finished in "
                + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms.");

        return jopUID;
    }

    public List<Serializable> getResultsFromWorker(final UUID jobUID, final int instanceID) {
        return jobResults.get(Pair.of(jobUID, instanceID));
    }

    public IConfig getConfig() { return config; }

    public int getNumberOfWorkers() { return workers.size(); }

    @Override
    public void deactivate() {
        netManager.deactivate();
        infraManager.deactivate();
        super.deactivate();
    }
}
