package de.tuberlin.pserver.client;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.events.ProgramFailureEvent;
import de.tuberlin.pserver.runtime.events.ProgramResultEvent;
import de.tuberlin.pserver.runtime.events.ProgramSubmissionEvent;
import de.tuberlin.pserver.runtime.usercode.UserCodeManager;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
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

    private final Map<UUID, CountDownLatch> activeJobs;

    private final Map<Pair<UUID,Integer>, List<Serializable>> jobResults;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerClient(final PServerClientFactory factory) {
        super(true, "PSERVER-CLIENT-THREAD");
        Preconditions.checkNotNull(factory);

        this.config          = factory.config;
        this.machine         = factory.machine;
        this.infraManager    = factory.infraManager;
        this.netManager      = factory.netManager;
        this.userCodeManager = factory.userCodeManager;
        this.activeJobs      = new HashMap<>();
        this.jobResults      = new HashMap<>();

        this.netManager.addEventListener(ProgramFailureEvent.PSERVER_FAILURE_JOB_EVENT, new JobFailureEvent());
        this.netManager.addEventListener(ProgramResultEvent.PSERVER_JOB_RESULT_EVENT, new JobResultEvent());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class JobFailureEvent implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final ProgramFailureEvent jfe = (ProgramFailureEvent)e;
            final List<Serializable> res = new ArrayList<>();
            res.add(jfe);
            jobResults.put(Pair.of(jfe.programID, jfe.nodeID), res);
            final CountDownLatch jobLatch = activeJobs.get(jfe.programID);
            if (jobLatch != null) {
                while (jobLatch.getCount() > 0)
                    jobLatch.countDown();
            } else {
                throw new IllegalStateException();
            }
        }
    }

    private final class JobResultEvent implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final ProgramResultEvent jre = (ProgramResultEvent) e;
            jobResults.put(Pair.of(jre.programID, jre.nodeID), jre.resultObjects);
            final CountDownLatch jobLatch = activeJobs.get(jre.programID);
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

    public UUID execute(final Class<? extends Program> jobClass) {
        Preconditions.checkNotNull(jobClass);

        final long start = System.nanoTime();
        final UUID jobUID = UUID.randomUUID();
        final List<Pair<String, byte[]>> byteCode = userCodeManager.extractClass(jobClass);
        final ProgramSubmissionEvent jobSubmission = new ProgramSubmissionEvent(
                machine,
                jobUID,
                byteCode
        );

        final CountDownLatch jobLatch = new CountDownLatch(infraManager.getMachines().size());
        activeJobs.put(jobUID, jobLatch);
        LOG.info("Submit Job '" + jobClass.getSimpleName() + "'.");
        infraManager.getMachines().forEach(md -> {
            if (!md.equals(machine)) netManager.sendEvent(md, jobSubmission);
        });

        try {
            jobLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        LOG.info("Job '" + jobClass.getSimpleName()
                + "' [" + jobSubmission.programID +"] finished in "
                + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms.");


        boolean programError = false;
        for (int i = 0; i < infraManager.getMachines().size(); ++i) {

            if (jobResults.get(Pair.of(jobUID, i)) == null)
                continue;

            if (jobResults.get(Pair.of(jobUID, i)).get(0) instanceof ProgramFailureEvent) {
                final ProgramFailureEvent pfe = (ProgramFailureEvent) jobResults.get(Pair.of(jobUID, i)).get(0);
                LOG.error(pfe.toString());
                programError = true;
            }
        }

        return !programError ? jobUID : null;
    }

    public List<Serializable> getResultsFromWorker(final UUID jobUID, final int nodeID) {
        return jobResults.get(Pair.of(jobUID, nodeID));
    }

    public IConfig getConfig() { return config; }

    public int getNumberOfWorkers() { return infraManager.getMachines().size(); }

    @Override
    public void deactivate() {
        netManager.deactivate();
        infraManager.deactivate();
        super.deactivate();
    }
}
