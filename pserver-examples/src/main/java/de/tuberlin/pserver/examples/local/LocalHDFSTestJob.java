package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.node.PServerNode;
import org.apache.log4j.ConsoleAppender;


public final class LocalHDFSTestJob {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_OF_MACHINES = 4;

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

        new ClusterSimulator(
                IConfigFactory.load(IConfig.Type.PSERVER_SIMULATION),
                PServerNode.class,
                true,
                NUM_OF_MACHINES,
                new String[] {"-Xmx1024m"}
        );

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new PServerClient(IConfigFactory.load(IConfig.Type.PSERVER_CLIENT)).execute(HDFSTestJob.class);

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}