package de.tuberlin.pserver.node;

import de.tuberlin.pserver.app.*;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.filesystem.FileSystemManager;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PServerNode extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(PServerNode.class);

    private final MachineDescriptor machine;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final FileSystemManager fileSystemManager;

    private final UserCodeManager userCodeManager;

    private final DataManager dataManager;

    private final ExecutionManager executionManager;

    private final ExecutorService executor;

    private final Map<UUID,PServerJobSubmissionEvent> jobMap;

    private CountDownLatch jobStartBarrier = null;

    private CountDownLatch jobEndBarrier = null;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerNode(final PServerNodeFactory factory) {
        super(true, "PSERVER-NODE-THREAD");

        this.machine            = factory.machine;
        this.infraManager       = factory.infraManager;
        this.netManager         = factory.netManager;
        this.fileSystemManager  = factory.fileSystemManager;
        this.userCodeManager    = factory.userCodeManager;
        this.dataManager        = factory.dataManager;
        this.executionManager   = factory.executionManager;
        this.jobMap             = new HashMap<>();
        this.executor           = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        netManager.addEventListener(PServerJobSubmissionEvent.PSERVER_JOB_SUBMISSION_EVENT, new PServerJobHandler());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {

        @Override
        public void handleEvent(final Event e) {
            final PServerJobSubmissionEvent jobSubmission = (PServerJobSubmissionEvent)e;
            LOG.info("Received job on instance " + "[" + infraManager.getNodeID() + "]" + jobSubmission.toString());
            jobMap.put(jobSubmission.jobUID, jobSubmission);
            jobStartBarrier = new CountDownLatch(1);
            jobEndBarrier   = new CountDownLatch(jobSubmission.perNodeParallelism);
            executionManager.createExecutionContext(jobSubmission.jobUID, jobSubmission.perNodeParallelism);

            final Class<?> clazz = userCodeManager.implantClass(jobSubmission.byteCode);

            if (!PServerJob.class.isAssignableFrom(clazz))
                throw new IllegalStateException();

            final Class<? extends PServerJob> jobClass = (Class<? extends PServerJob>) clazz;

            final JobContext jobContext = new JobContext(
                    jobSubmission.clientMachine,
                    jobSubmission.jobUID,
                    clazz.getName(),
                    clazz.getSimpleName(),
                    infraManager.getMachines().size(),
                    jobSubmission.perNodeParallelism,
                    infraManager.getNodeID(),
                    netManager,
                    DHT.getInstance(),
                    dataManager,
                    executionManager
            );

            dataManager.registerJob(jobContext.jobUID, jobContext);

            for (int i = 0; i < jobContext.perNodeParallelism; ++i) {
                final int threadID = i;
                executor.execute(() -> {
                    try {
                        final PServerJob jobInvokeable = jobClass.newInstance();
                        final InstanceContext instanceContext = new InstanceContext(
                                jobContext,
                                threadID,
                                jobInvokeable
                        );

                        dataManager.registerInstanceContext(instanceContext);
                        jobContext.addInstance(instanceContext);
                        jobInvokeable.injectContext(instanceContext);
                        executeLifecycle(jobInvokeable);

                        if (threadID == 0) {
                            try {
                                jobEndBarrier.await();
                            } catch (InterruptedException ie) {
                            }
                            final List<Serializable> results = dataManager.getResults(jobSubmission.jobUID);
                            final PServerJobResultEvent jre = new PServerJobResultEvent(
                                    machine,
                                    infraManager.getNodeID(),
                                    jobSubmission.jobUID,
                                    results
                            );
                            netManager.sendEvent(jobSubmission.clientMachine, jre);
                            dataManager.unregisterJob(jobContext.jobUID);
                            executionManager.deleteExecutionContext(jobSubmission.jobUID);
                        }

                        jobContext.removeInstance(instanceContext);
                        dataManager.unregisterInstanceContext();

                    } catch (Exception ex) {
                        final PServerJobFailureEvent jfe = new PServerJobFailureEvent(
                                machine,
                                jobSubmission.jobUID,
                                infraManager.getNodeID(),
                                threadID, clazz.getSimpleName(),
                                ex.getCause()
                        );
                        netManager.sendEvent(jobSubmission.clientMachine, jfe);
                    }
                });
            }
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void deactivate() {
        netManager.deactivate();
        infraManager.deactivate();
        super.deactivate();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void executeLifecycle(final PServerJob job) {
        try {

            if (job.getInstanceContext().instanceID == 0) {
                {
                    LOG.info("Enter " + job.instanceContext.jobContext.simpleClassName + " prologue phase.");

                    final long start = System.currentTimeMillis();

                    job.prologue();

                    dataManager.postProloguePhase(job.getInstanceContext());

                    final long end = System.currentTimeMillis();

                    LOG.info("Leave " + job.instanceContext.jobContext.simpleClassName
                            + " prologue phase [duration: " + (end - start) + " ms].");
                }

                Thread.sleep(5000); // TODO: Not very elegant...

                jobStartBarrier.countDown();
            }

            jobStartBarrier.await();

            {
                LOG.info("Enter " + job.instanceContext.jobContext.simpleClassName + " computation phase.");

                final long start = System.currentTimeMillis();

                job.compute();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + job.instanceContext.jobContext.simpleClassName +
                        " computation phase [duration: " + (end - start) + " ms].");
            }

            if (job.getInstanceContext().instanceID == 0) {
                {
                    LOG.info("Enter " + job.instanceContext.jobContext.simpleClassName + " epilogue phase.");

                    final long start = System.currentTimeMillis();

                    job.epilogue();

                    final long end = System.currentTimeMillis();

                    LOG.info("Leave " + job.instanceContext.jobContext.simpleClassName
                            + " epilogue phase [duration: " + (end - start) + " ms].");
                }
            }

            jobEndBarrier.countDown();

        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }
}
