package de.tuberlin.pserver.examples.playground;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.math.Matrix;

import java.util.concurrent.CyclicBarrier;

public class APIDesign {

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

        private final DataManager dataManager;

        private static CyclicBarrier internalSyncBarrier;

        private int staleness = -1;

        private int epoch = 0;

        // ---------------------------------------------------

        public Iteration(final DataManager dataManager) {
            this.dataManager = Preconditions.checkNotNull(dataManager);
        }

        // ---------------------------------------------------

        public void staleness(final int staleness) { this.staleness = staleness; }

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
}