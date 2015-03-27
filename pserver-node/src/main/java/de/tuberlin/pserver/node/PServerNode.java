package de.tuberlin.pserver.node;

import de.tuberlin.pserver.app.*;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.filesystem.HDFSManager;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PServerNode extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(PServerNode.class);

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final HDFSManager hdfsManager;

    private final UserCodeManager userCodeManager;

    private final DataManager dataManager;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PServerNode(final IConfig config) {
        super(true, "PSERVER-NODE-THREAD");

        MachineDescriptor machine;
        try {
            machine = new MachineDescriptor(
                    UUID.randomUUID(),
                    InetHelper.getIPAddress(),
                    InetHelper.getFreePort(),
                    InetAddress.getLocalHost().getHostName()
            );
        } catch(Throwable t) {
            throw new IllegalStateException(t);
        }

        this.infraManager       = new InfrastructureManager(machine, config);
        this.netManager         = new NetManager(machine, infraManager, 16);
        this.userCodeManager    = new UserCodeManager(this.getClass().getClassLoader(), false);

        final RPCManager rpcManager = new RPCManager(netManager);

        this.infraManager.addEventListener(ZookeeperClient.IM_EVENT_NODE_ADDED, event -> {
            if (event.getPayload() instanceof MachineDescriptor) {
                final MachineDescriptor md = (MachineDescriptor)event.getPayload();
                netManager.connectTo(md);
            } else
                throw new IllegalStateException();
        });

        this.infraManager.start();

        netManager.addEventListener(PServerJobDescriptor.PSERVER_SUBMIT_JOB_EVENT, new PServerJobHandler());

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            throw new IllegalStateException();
        }

        //if (infraManager.getCurrentMachineIndex() == 0)
        //    hdfsManager = new HDFSManager(infraManager, netManager, rpcManager);
        //else
            hdfsManager = null;

        LOG.info(infraManager.getMachine()
                + " | " + infraManager.getCurrentMachineIndex()
                + " | " + infraManager.getActivePeers().size()
                + " | " + infraManager.getMachines().size());

        DHT.getInstance().initialize(infraManager, netManager);
        this.dataManager = new DataManager(infraManager, netManager, rpcManager, hdfsManager, DHT.getInstance());
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class PServerJobHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            final PServerJobDescriptor job = (PServerJobDescriptor)e.getPayload();
            executor.execute(() -> {
                final Class<?> clazz = userCodeManager.implantClass(job);
                if (clazz != null) {
                    if (PServerJob.class.isAssignableFrom(clazz)) {
                        @SuppressWarnings("unchecked")
                        final Class<? extends PServerJob> jobClass = (Class<? extends PServerJob>) clazz;
                        try {
                            final PServerContext ctx = new PServerContext(
                                    infraManager.getCurrentMachineIndex(),
                                    job,
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

        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        // construct base argument parser
        ArgumentParser parser = getArgumentParser();
        try {
            // parse the arguments and store them as system properties
            parser.parseArgs(args).getAttrs().entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .forEach(e -> System.setProperty(e.getKey(), e.getValue().toString()));

            // Start the PServer node.
            long start = System.nanoTime();
            new PServerNode(IConfigFactory.load(IConfig.Type.PSERVER_NODE));
            LOG.info("pserver startup: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms");
        } catch (HelpScreenException e) {
            parser.handleError(e);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Throwable e) {
            System.err.println(String.format("Unexpected error: %s", e));
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static ArgumentParser getArgumentParser() {
        //@formatter:off
        ArgumentParser parser = ArgumentParsers.newArgumentParser("pserver-node")
                .defaultHelp(true)
                .description("pserver");

        parser.addArgument("--config-dir")
                .type(new FileArgumentType().verifyIsDirectory().verifyCanRead())
                .dest("aura.path.config")
                .setDefault("config")
                .metavar("PATH")
                .help("config folder");
        //@formatter:on

        return parser;
    }
}
