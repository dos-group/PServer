package de.tuberlin.pserver.node;

import de.tuberlin.pserver.app.*;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.filesystem.hdfs.HDFSManager;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    private final HDFSManager hdfsManager;

    private final UserCodeManager userCodeManager;

    private final DataManager dataManager;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final Map<UUID,PServerJobDescriptor> jobMap;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerNode(final PServerNodeFactory factory) {
        super(true, "PSERVER-NODE-THREAD");

        this.machine         = factory.machine;
        this.infraManager    = factory.infraManager;
        this.netManager      = factory.netManager;
        this.hdfsManager     = factory.hdfsManager;
        this.userCodeManager = factory.userCodeManager;
        this.dataManager     = factory.dataManager;
        this.jobMap          = new HashMap<>();

        netManager.addEventListener(PServerJobDescriptor.PSERVER_SUBMIT_JOB_EVENT, new PServerJobHandler());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final PServerJobDescriptor jobDescriptor = (PServerJobDescriptor)e.getPayload();
            LOG.info("Received jobDescriptor " + jobDescriptor.simpleClassName + " on node " + machine + ".");
            jobMap.put(jobDescriptor.jobUID, jobDescriptor);
            executor.execute(() -> {
                final Class<?> clazz = userCodeManager.implantClass(jobDescriptor);
                if (clazz != null) {
                    if (PServerJob.class.isAssignableFrom(clazz)) {
                        @SuppressWarnings("unchecked")
                        final Class<? extends PServerJob> jobClass = (Class<? extends PServerJob>) clazz;
                        try {
                            final PServerContext ctx = new PServerContext(
                                    infraManager.getCurrentMachineIndex(),
                                    jobDescriptor,
                                    DHT.getInstance(),
                                    netManager,
                                    dataManager
                            );
                            final PServerJob jobInvokeable = jobClass.newInstance();
                            jobInvokeable.injectContext(ctx);
                            executeLifecycle(jobInvokeable);
                        } catch (Exception ex) {
                            throw new IllegalStateException(ex);
                        }
                    }
                } else
                    throw new IllegalStateException();
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void shutdown() {
        netManager.shutdown();
        infraManager.shutdown();
        shutdownEventDispatcher();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void executeLifecycle(final PServerJob job) {
        try {

            job.begin();

            if (hdfsManager != null)
                hdfsManager.computeInputSplits();

            job.compute();

            job.end();

            final NetEvents.NetEvent finishEvent = new NetEvents.NetEvent(PServerJobDescriptor.PSERVER_FINISH_JOB_EVENT);
            finishEvent.setPayload(job.getJobContext().jobDescriptor.jobUID);
            netManager.sendEvent(job.getJobContext().jobDescriptor.clientMachine, finishEvent);

        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }
}
