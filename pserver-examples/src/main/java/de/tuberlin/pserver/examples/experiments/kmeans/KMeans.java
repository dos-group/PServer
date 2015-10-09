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
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.Dense64Matrix;
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
    public Matrix matrix;

    @State(scope = Scope.REPLICATED,
            rows = K,
            cols = COLS + 1
    )
    public Matrix centroidsUpdate;

    public final Matrix centroids = new Dense64Matrix(K, COLS);

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "centroidsUpdate", type = TransactionType.PULL)
    public final TransactionDefinition centroidsUpdateSync = new TransactionDefinition(

            (Apply<Matrix, Void>) (updates) -> {
                for (final Matrix update : updates) {
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

        lifecycle.preProcess(() -> {

            Random rand = new Random(42);
            double[] data = new double[(int)(K * COLS)];
            for (int i = 0; i < K * COLS; i++) {
                data[i] = rand.nextDouble();
            }

            centroids.setArray(data);


        }).process(() -> {

            centroidsUpdate.assign(0);

            UnitMng.loop(10, Loop.BULK_SYNCHRONOUS, (iteration) -> {
                int nodeId = programContext.runtimeContext.nodeID;
                // BEGIN: PULL MODEL FROM OTHER NODES AND MERGE
                //System.out.println(nodeId + ": pre pull centroidsUpdate: " + centroidsUpdate);
                /*TransactionMng.commit(centroidsUpdateSync);
                //System.out.println(nodeId + ": post pull centroidsUpdate: " + centroidsUpdate);
                for (int i = 0; i < K; i++) {
                    if (centroidsUpdate.get(i, COLS) > 0) {
                        Matrix update = centroidsUpdate.getRow(i, 0, COLS);
                        if (centroidsUpdate.get(i, COLS) > 0) {
                            centroids.assignRow(i, update.scale(1. / centroidsUpdate.get(i, COLS), update));
                        }
                    }
                }
                centroidsUpdate.assign(0);*/
                // END: PULL MODEL FROM OTHER NODES AND MERGE

                // BEGIN: STANDARD KMEANS ON LOCAL PARTITION+

                //atomic(state(matrix), () -> {
                    Matrix.RowIterator iter = matrix.rowIterator();
                    while (iter.hasNext()) {
                        Matrix point = iter.get();
                        iter.next();
                        System.out.println("current point: " + point);
                        double closestDistance = Double.MAX_VALUE;
                        long closestCentroidId = -1;
                        for (long centroidId = 0; centroidId < K; centroidId++) {
                            Matrix centroid = centroids.getRow(centroidId);
                            System.out.println("current centroid: " + centroid);
                            Matrix diff = centroid.sub(point);
                            double distance = diff.norm(2);
                            if (distance < closestDistance) {
                                closestDistance = distance;
                                closestCentroidId = centroidId;
                            }
                        }
                        System.out.println("closest centroid: " + closestCentroidId);
                        Matrix updateDelta = point.copy(1, COLS + 1);
                        updateDelta.set(0, COLS, 1);

                        centroidsUpdate.assignRow(closestCentroidId, centroidsUpdate.getRow(closestCentroidId).add(updateDelta));
                    }
                //});

                //System.out.println();

                // END: STANDARD KMEANS ON LOCAL PARTITION

                // BEGIN: PULL MODEL FROM OTHER NODES AND MERGE
                System.out.println(nodeId + ": pre pull centroidsUpdate: " + centroidsUpdate);
                TransactionMng.commit(centroidsUpdateSync);
                System.out.println(nodeId + ": post pull centroidsUpdate: " + centroidsUpdate);
                for (int i = 0; i < K; i++) {
                    if (centroidsUpdate.get(i, COLS) > 0) {
                        Matrix update = centroidsUpdate.getRow(i, 0, COLS);
                        if (centroidsUpdate.get(i, COLS) > 0) {
                            centroids.assignRow(i, update.scale(1. / centroidsUpdate.get(i, COLS), update));
                        }
                    }
                }
                centroidsUpdate.assign(0);
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

    public static void cluster() {
        System.setProperty("pserver.profile", "wally");
        PServerExecutor.DISTRIBUTED
                .run(KMeans.class)
                .done();
    }

    public static void local() {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(KMeans.class)
                .done();
    }
}

