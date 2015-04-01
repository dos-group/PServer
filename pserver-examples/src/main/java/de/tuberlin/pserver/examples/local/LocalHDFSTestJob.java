package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.node.PServerMain;
import org.apache.log4j.ConsoleAppender;


public final class LocalHDFSTestJob {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    //public static final String HDFS_SOURCE_FILE_PATH = "/tmp/input/groups";

    // ---------------------------------------------------

    private LocalHDFSTestJob() {}

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class HDFSTestJob extends PServerJob {

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void compute() {

            if (ctx.instanceID == 0) {
                ctx.dataManager.getNextInputSplit();
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender());

        final ClusterSimulator simulator = new ClusterSimulator(
                IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION),
                PServerMain.class
        );

        final PServerClient client = PServerClientFactory.createPServerClient();

        client.execute(HDFSTestJob.class);

        client.shutdown();

        simulator.shutdown();
    }
}