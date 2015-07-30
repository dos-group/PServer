package de.tuberlin.pserver.examples.playground;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;

import java.util.concurrent.CyclicBarrier;

public class APIDesign {

    // ---------------------------------------------------
    // Control Flow Factory.
    // ---------------------------------------------------

    public static class ControlFlow {

        private static DataManager dataManager;

        public static void init(final DataManager dataManager) { ControlFlow.dataManager = dataManager; }

        public static Iteration makeIteration() { return new Iteration(dataManager); }
    }

    // ---------------------------------------------------
    // Iteration API.
    // ---------------------------------------------------

    public static interface IterationBody {

        public abstract void body();
    }

    public static interface RowMatrixIterationBody {

        public abstract void body(Matrix.RowIterator iter);
    }

    public static interface IterationTermination {

        public abstract boolean terminate();
    }

    public static class Iteration {

        // ---------------------------------------------------

        private final DataManager dataManager;

        private static CyclicBarrier internalSyncBarrier;

        private int staleness = -1;

        private int epoch = 0;

        // ---------------------------------------------------

        public Iteration(final DataManager dataManager) {
            this.dataManager = Preconditions.checkNotNull(dataManager);
        }

        // ---------------------------------------------------

        public Iteration staleness(final int staleness) { this.staleness = staleness; return this; }

        public void iterate(final IterationTermination t, final IterationBody b) {

            while (!t.terminate()) {

                b.body();

                externalSync();

                ++epoch;
            }
        }

        public void iterate(final int n, final IterationBody b) {

            for (epoch = 0; epoch < n; ++epoch) {

                b.body();

                externalSync();
            }
        }

        public void iterate(final Matrix m, final RowMatrixIterationBody b) {

            final Matrix.RowIterator iter = dataManager.createThreadPartitionedRowIterator(m);

            while (iter.hasNextRow()) {

                iter.nextRow();

                b.body(iter);

                externalSync();

                ++epoch;
            }
        }

        private void internalSync() {
            try {
                internalSyncBarrier.await();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private void externalSync() {
            dataManager.sync(staleness);
        }
    }

    // ---------------------------------------------------
    // Job.
    // ---------------------------------------------------

    public class APIDesignJob extends PServerJob {

        private Matrix X;

        @Override
        public void prologue() {

            ControlFlow.init(ctx.dataManager);

            final Matrix X = new MatrixBuilder()
                    .dimension(1000, 1000)
                    .format(Matrix.Format.DENSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .build();

            dataManager.putObject("X", X);
        }

        @Override
        public void compute() {

            X = dataManager.getObject("X");

            ControlFlow.makeIteration()
                    .iterate(15, () -> {

                        ControlFlow.makeIteration()
                                .iterate(X, (iter) -> {

                                    
                                });
                    });
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(APIDesignJob.class, 4)
                .done();
    }
}