package de.tuberlin.pserver.examples.experiments.kmeans;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.runtime.filesystem.record.RowRecordIteratorProducer;
import de.tuberlin.pserver.runtime.mcruntime.Parallel;

import java.util.Random;

public class KMeans extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final long ROWS = 1000;

    private static final long COLS = 2;

    private static final int K = 2;

    private static final String FILE = "datasets/stripes2.csv";

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED,
            rows = ROWS,
            cols = COLS,
            path = FILE,
            format = Format.DENSE_FORMAT,
            recordFormat = RowRecordIteratorProducer.class
    )
    public Matrix64F data;

    @State(scope = Scope.REPLICATED,
            rows = K,
            cols = COLS + 1
    )
    public Matrix64F centroidsUpdate;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "centroidsUpdate", type = TransactionType.PULL)
    public final TransactionDefinition centroidsUpdateSync = new TransactionDefinition(

            (Apply<Matrix64F, Void>) (updates) -> {
                for (final Matrix64F update : updates) {
                    Parallel.For(update, (i, j, v) -> centroidsUpdate.set(i, j, centroidsUpdate.get(i, j) + update.get(i, j)));
                }
                return null;
            }
    );

    // ---------------------------------------------------
    // Units.
    // --------------------------------------------------

    @Unit
    public void main(final Lifecycle lifecycle) {

        final Matrix64F centroids = new DenseMatrix64F(K, COLS);

        lifecycle.preProcess(() -> {

            Random rand = new Random(42);
            double[] data = new double[(int)(K * COLS)];
            for (int i = 0; i < K * COLS; i++) {
                data[i] = rand.nextDouble();
            }

            centroids.setArray(data);

        }).process(() -> {

            centroidsUpdate.assign(0.);

            UnitMng.loop(10, Loop.BULK_SYNCHRONOUS, (iteration) -> {

                // BEGIN: STANDARD KMEANS ON LOCAL PARTITION+
                atomic(state(data), () -> {
                    Matrix64F.RowIterator it = data.rowIterator();
                    while (it.hasNext()) {

                        final Matrix64F point = it.get();
                        it.next();
                        double closestDistance = Double.MAX_VALUE;
                        long closestCentroidId = -1;
                        for (long centroidId = 0; centroidId < K; centroidId++) {
                            double distance = centroids.getRow(centroidId).sub(point).norm(2);
                            if (distance < closestDistance) {
                                closestDistance = distance;
                                closestCentroidId = centroidId;
                            }
                        }
                        Matrix64F updateDelta = point.copy(1, COLS + 1);
                        updateDelta.set(0, COLS, 1.);

                        centroidsUpdate.assignRow(closestCentroidId, centroidsUpdate.getRow(closestCentroidId).add(updateDelta));
                    }
                });
                // END: STANDARD KMEANS ON LOCAL PARTITION

                // BEGIN: PULL MODEL FROM OTHER NODES AND MERGE
                TransactionMng.commit(centroidsUpdateSync);

                for (int i = 0; i < K; i++) { // TODO: MOVE TO APPLY PHASE OF TRANSACTION!
                    if (centroidsUpdate.get(i, COLS) > 0) {
                        final Matrix64F update = centroidsUpdate.getRow(i, 0, COLS);
                        if (centroidsUpdate.get(i, COLS) > 0) {
                            centroids.assignRow(i, update.scale(1. / centroidsUpdate.get(i, COLS), update));
                        }
                    }
                }
                centroidsUpdate.assign(0.);
                // END: PULL MODEL FROM OTHER NODES AND MERGE

            });

        }).postProcess(() -> {
            for (int i = 0; i < K; i++) {
                int nodeId = programContext.runtimeContext.nodeID;
                System.out.println("centroid[node:" + nodeId + ",row:" + i + "]=" + centroids.getRow(i));
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) {
        local();
    }

    // ---------------------------------------------------

    public static void cluster() {
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.REMOTE
                .run(KMeans.class)
                .done();
    }

    public static void local() {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(KMeans.class)
                .done();
    }
}
