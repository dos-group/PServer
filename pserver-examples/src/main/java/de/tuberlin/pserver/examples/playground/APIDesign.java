package de.tuberlin.pserver.examples.playground;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.MObject;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;

import java.util.concurrent.CyclicBarrier;

public class APIDesign {

    // ---------------------------------------------------
    // Data Flow.
    // ---------------------------------------------------

    public static class DF {

        private static DataManager dataManager;

        public static void init(final DataManager dataManager) { CF.dataManager = dataManager; }

        public static <T extends MObject> Key put(final String name, final T obj) {
            return dataManager.putObject(name, obj);
        }

        public static <T extends MObject> T get(final String name) {
            return dataManager.getObject(name);
        }
    }

    // ---------------------------------------------------
    // Control Flow.
    // ---------------------------------------------------

    public static class CF {

        private static DataManager dataManager;

        public static void init(final DataManager dataManager) { CF.dataManager = dataManager; }

        public static int numNodes() { return -1; }

        public static int numCores() { return -1; }

        public static Iteration iterate() { return new Iteration(dataManager); }

        public static Selection select() { return new Selection(dataManager); }
    }

    // ---------------------------------------------------
    //  Control Flow Constructs.
    // ---------------------------------------------------

    public static interface Body {

        public abstract void body();
    }

    // ---------------------------------------------------


    public static class Selection {

        // ---------------------------------------------------

        private final DataManager dataManager;

        private int fromNodeID;

        private int toNodeID;

        private int formThreadID;

        private int toThreadID;

        // ---------------------------------------------------

        public Selection(final DataManager dataManager) {
            this.dataManager = Preconditions.checkNotNull(dataManager);
        }

        // ---------------------------------------------------

        public Selection node(final int nodeID) {


            return this;
        }

        public Selection node(final int formNodeID, final int toNodeID) {


            return this;
        }

        public Selection allNodes() {


            return this;
        }

        public Selection core(final int threadID) {


            return this;
        }

        public Selection core(final int formThreadID, final int toThreadID) {


            return this;
        }

        public Selection allCores() {


            return this;
        }

        // ---------------------------------------------------

        public Selection sync() { return this; }

        // ---------------------------------------------------


        public void execute(final Body body) {

            if (fromNodeID != -1 && toNodeID != -1) {

                body.body();

            } else {

                if (fromNodeID != -1 && toNodeID == -1) {

                    body.body();

                } else {

                    throw new IllegalStateException();
                }
            }
        }
    }

    // ---------------------------------------------------

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

        public Iteration stale(final int staleness) { this.staleness = staleness; return this; }

        public Iteration sync() { this.staleness = 1; return this; }

        public Iteration async() { this.staleness = -1; return this; }

        // ---------------------------------------------------

        public void execute(final IterationTermination t, final Body b) {

            while (!t.terminate()) {

                b.body();

                externalSync();

                ++epoch;
            }
        }

        public void execute(final int n, final Body b) {

            for (epoch = 0; epoch < n; ++epoch) {

                b.body();

                externalSync();
            }
        }

        public void execute(final Matrix m, final RowMatrixIterationBody b) {

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
            dataManager.globalSync(staleness);
        }
    }

    // ---------------------------------------------------
    // Job.
    // ---------------------------------------------------

    public class APIDesignJob extends PServerJob {

        private Matrix X;

        @Override
        public void prologue() {

            DF.init(instanceContext.jobContext.dataManager);
            CF.init(instanceContext.jobContext.dataManager);

            final Matrix X = new MatrixBuilder()
                    .dimension(1000, 1000)
                    .format(Matrix.Format.DENSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .build();

            DF.put("X", X);
        }

        @Override
        public void compute() {

            X = DF.get("X");

            CF.iterate()
                    .async()
                    .execute(15, () -> {

                        CF.select()
                                .allNodes()
                                .core(0)
                                .execute(() -> {

                                    CF.iterate()
                                            .execute(X, (iter) -> {

                                            });
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