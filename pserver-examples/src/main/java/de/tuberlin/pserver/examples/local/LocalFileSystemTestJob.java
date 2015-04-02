package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.node.PServerMain;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.ConsoleAppender;


public final class LocalFileSystemTestJob {

    private LocalFileSystemTestJob() {}

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class FileSystemTestJob extends PServerJob {

        private FileDataIterator<CSVRecord> csvFileIterator;

        @Override
        public void begin() {
            csvFileIterator = ctx.dataManager.createFileIterator("datasets/covtype.data", CSVRecord.class);
        }

        @Override
        public void compute() {
            while (csvFileIterator.hasNext())
                System.out.println(ctx.instanceID + " => " + csvFileIterator.next());
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

        client.execute(FileSystemTestJob.class);

        client.shutdown();

        simulator.shutdown();
    }
}