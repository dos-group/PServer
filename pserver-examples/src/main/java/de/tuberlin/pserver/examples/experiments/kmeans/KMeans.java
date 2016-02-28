package de.tuberlin.pserver.examples.experiments.kmeans;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.runtime.core.config.ConfigLoader;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

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

    @Load(filePath = "datasets/stripes2.csv")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS)
    public Matrix32F data;

    @Matrix(scheme = DistScheme.REPLICATED, rows = K, cols = COLS + 1)
    public Matrix32F centroidsUpdate;

    public final Matrix32F centroids = new DenseMatrix32F(K, COLS);

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "centroidsUpdate", type = TransactionType.PUSH)
    public final TransactionDefinition centroidsUpdateSync = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, remoteUpdates, localState) -> {
                for (final Matrix32F update : remoteUpdates) {
                    Parallel.For(update, (i, j, v) -> centroidsUpdate.set(i, j, centroidsUpdate.get(i, j) + update.get(i, j)));
                }
                for (int i = 0; i < K; i++) {
                    if (centroidsUpdate.get(i, COLS) > 0) {
                        final Matrix32F update = centroidsUpdate.getRow(i, 0, COLS);
                        if (centroidsUpdate.get(i, COLS) > 0) {
                            centroids.assignRow(i, update.scale(1f / centroidsUpdate.get(i, COLS), update));
                        }
                    }
                }
                centroidsUpdate.assign(0f);
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

            Random rand = new Random(42);
            float[] data = new float[(int)(K * COLS)];
            for (int i = 0; i < K * COLS; i++) {
                data[i] = (float)rand.nextDouble();
            }

            // centroids.setArray(data); // TODO: SOLVE THAT PROBLEM!

        }).process(() -> {

            centroidsUpdate.assign(0f);

            UnitMng.loop(10, Loop.BULK_SYNCHRONOUS, (iteration) -> {

                // BEGIN: STANDARD KMEANS ON LOCAL PARTITION+
                atomic(state(data), () -> {
                    Matrix32F.RowIterator it = data.rowIterator();
                    while (it.hasNext()) {

                        final Matrix32F point = it.get();
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
                        Matrix32F updateDelta = point.copy(1, COLS + 1);
                        updateDelta.set(0, COLS, 1f);

                        centroidsUpdate.assignRow(closestCentroidId, centroidsUpdate.getRow(closestCentroidId).add(updateDelta));
                    }
                });
                // END: STANDARD KMEANS ON LOCAL PARTITION

                // Pull model from remote srcStateObjectNodes and merge.
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
    // EntryImpl Point.
    // ---------------------------------------------------

    public static void main(String[] args) { local(); }

    // ---------------------------------------------------

    private static void cluster() {
        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), KMeans.class)
                .done();
    }

    private static void local() {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(KMeans.class)
                .done();
    }
}
