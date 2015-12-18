package de.tuberlin.pserver.examples.experiments.kmeans;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.runtime.filesystem.Format;
import de.tuberlin.pserver.runtime.parallel.Parallel;

import java.util.Random;

public class KMeans extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final long ROWS = 1000;

    private static final long COLS = 2;

    private static final int K = 2;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED,
            rows = ROWS,
            cols = COLS,
            path = "datasets/stripes2.csv",
            format = Format.DENSE_FORMAT
            //recordFormat = RowRecordIteratorProducer.class
    )
    public Matrix64F data;

    @State(scope = Scope.REPLICATED,
            rows = K,
            cols = COLS + 1
    )
    public Matrix64F centroidsUpdate;

    public final Matrix64F centroids = new DenseMatrix64F(K, COLS);

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "centroidsUpdate", type = TransactionType.PUSH)
    public final TransactionDefinition centroidsUpdateSync = new TransactionDefinition(

            (Update<Matrix64F>) (remoteUpdates, localState) -> {

                for (final Matrix64F update : remoteUpdates) {

                    Parallel.For(update, (i, j, v) -> centroidsUpdate.set(i, j, centroidsUpdate.get(i, j) + update.get(i, j)));
                }

                for (int i = 0; i < K; i++) {
                    if (centroidsUpdate.get(i, COLS) > 0) {
                        final Matrix64F update = centroidsUpdate.getRow(i, 0, COLS);
                        if (centroidsUpdate.get(i, COLS) > 0) {
                            centroids.assignRow(i, update.scale(1. / centroidsUpdate.get(i, COLS), update));
                        }
                    }
                }

                centroidsUpdate.assign(0.);
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void main(final Lifecycle lifecycle) {

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

                // Pull model from remote stateObjectNodes and merge.
                TransactionMng.commit(centroidsUpdateSync);
            });

        }).postProcess(() -> {
            for (int i = 0; i < K; i++) {
                int nodeId = programContext.nodeID;
                System.out.println("centroid[node:" + nodeId + ",row:" + i + "]=" + centroids.getRow(i));
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) { local(); }

    // ---------------------------------------------------

    private static void cluster() {
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.REMOTE
                .run(KMeans.class)
                .done();
    }

    private static void local() {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(KMeans.class)
                .done();
    }
}
