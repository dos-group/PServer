package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.dht.valuetypes.DoubleBufferValue;
import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.client.PServerClientFactory;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.ml.optimization.naive.GradientDescent;
import de.tuberlin.pserver.node.PServerMain;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.ConsoleAppender;

public final class LocalAsyncSGDTestJob {

    private LocalAsyncSGDTestJob() {}

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int MTX_ROWS = 1;

    private static final int MTX_COLS = 16;

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class AsyncSGDTestJob extends PServerJob {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private FileDataIterator<CSVRecord> csvFileIterator;

        private GradientDescent.SGDRegressor regressor;

        private final DataManager.MatrixMerger<DoubleBufferValue> merger = (a,b) -> {
                        for (int i = 0; i < a.numRows(); ++i) {
                            for (int j = 0; j < a.numCols(); ++j) {
                                final double v = (a.get(i,j) + b.get(i,j)) / 2;
                                a.set(i, j, v);
                            }
                        }
                    };

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void prologue() {
            csvFileIterator = ctx.dataManager.createFileIterator("datasets/data.csv", CSVRecord.class);
            regressor = new GradientDescent.SGDRegressor();
            regressor.setLossFunction(new GradientDescent.SquaredLossFunction());
            regressor.setLearningRate(0.005);
            regressor.setNumIterations(400);
        }

        @Override
        public void compute() {
            final long start = System.currentTimeMillis();
            DoubleBufferValue m = ctx.dataManager.createLocalMatrix("model1", MTX_ROWS, MTX_COLS, DoubleBufferValue.BlockLayout.ROW_LAYOUT);
            regressor.setWeightsUpdater((epoch, weights) -> {
                if (epoch % 10 == 0)
                    ctx.dataManager.mergeMatrix(m, merger);
            });
            regressor.fit(m.toDoubleArray(), csvFileIterator, 15);
            final long end = System.currentTimeMillis();

            LOG.info("FINISHED JOB ON INSTANCE ["+ ctx.instanceID +"] IN " + ((end - start) / 1000) + "s");
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

        client.execute(AsyncSGDTestJob.class);

        client.shutdown();

        simulator.shutdown();
    }
}