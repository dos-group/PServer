package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerClient;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.config.IConfigFactory;
import de.tuberlin.pserver.core.infra.ClusterSimulator;
import de.tuberlin.pserver.math.experimental.types.matrices.DenseDoubleMatrix;
import de.tuberlin.pserver.node.PServerNode;
import org.apache.log4j.ConsoleAppender;

import java.util.Random;

public final class LocalAsyncSGDTestJob {

    private LocalAsyncSGDTestJob() {}

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_OF_MACHINES = 4;

    private static final int MTX_ROWS = 2000;

    private static final int MTX_COLS = 2000;

    // ---------------------------------------------------
    // Jobs.
    // ---------------------------------------------------

    public static final class AsyncSGDTestJob extends PServerJob {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final Random rand = new Random();

        private final DataManager.MatrixMerger<DenseDoubleMatrix> merger = (a,b) -> {
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
        public void compute() {
            final long start = System.currentTimeMillis();
            DenseDoubleMatrix m = ctx.dataManager.createLocalMatrix("model1", MTX_ROWS, MTX_COLS);
            for (int i = 0; i < 100000; ++i) {
                computeGradient(m);
                if (i % 1000 == 0)
                    ctx.dataManager.mergeMatrix(m, merger);
            }
            final long end = System.currentTimeMillis();
            System.out.println("FINISHED JOB ON INSTANCE ["+ ctx.instanceID +"] IN " + ((end - start) / 1000) + "s");
        }

        // ---------------------------------------------------
        // Private Methods.
        // ---------------------------------------------------

        private void computeGradient(final DenseDoubleMatrix m) {
            int x = rand.nextInt(MTX_ROWS - 1);
            int y = rand.nextInt(MTX_COLS - 1);
            double v = rand.nextDouble();
            m.set(x, y, v);
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

        new PServerClient(IConfigFactory.load(IConfig.Type.PSERVER_CLIENT)).execute(AsyncSGDTestJob.class);

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}