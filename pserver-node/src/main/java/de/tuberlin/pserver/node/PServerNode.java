package de.tuberlin.pserver.node;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.PServerInvokeable;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.UserCodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
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

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final UserCodeManager userCodeManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerNode() {
        super(true, "PSERVER-NODE-THREAD");
        this.machine = new MachineDescriptor(UUID.randomUUID(), InetHelper.getIPAddress(), InetHelper.getFreePort());
        this.infraManager = new InfrastructureManager(machine);
        this.netManager = new NetManager(machine, 16);
        this.userCodeManager = new UserCodeManager(this.getClass().getClassLoader(), false);

        this.infraManager.addEventListener(ZookeeperClient.IM_EVENT_NODE_ADDED, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                if (event.getPayload() instanceof MachineDescriptor) {
                    final MachineDescriptor md = (MachineDescriptor)event.getPayload();
                    netManager.connectTo(md);
                } else
                    throw new IllegalStateException();
            }
        });

        this.infraManager.start();

        netManager.addEventListener(PServerJob.PSERVER_SUBMIT_JOB_EVENT, new PServerJobHandler());

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
        }

        LOG.info(infraManager.getMachine()
                + " | " + infraManager.getCurrentMachineIndex()
                + " | " + infraManager.getActivePeers().size()
                + " | " + infraManager.getMachines().size());

        DHT.getInstance().initialize(infraManager, netManager);
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final PServerJob job = (PServerJob)e.getPayload();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final Class<?> clazz = userCodeManager.implantClass(job);;
                    Preconditions.checkState(clazz != null);
                    if (PServerInvokeable.class.isAssignableFrom(clazz)) {
                        @SuppressWarnings("unchecked")
                        final Class<? extends PServerInvokeable> jobClass = (Class<? extends PServerInvokeable>) clazz;
                        try {
                            final PServerInvokeable jobInvokeable = jobClass.newInstance();
                            execute(jobInvokeable);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }

                }
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void shutdown() {
        netManager.shutdown();
    }

    public void execute(final PServerInvokeable invokeable) {
        try {
            invokeable.invoke(infraManager.getCurrentMachineIndex());
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        new PServerNode();
    }
}
