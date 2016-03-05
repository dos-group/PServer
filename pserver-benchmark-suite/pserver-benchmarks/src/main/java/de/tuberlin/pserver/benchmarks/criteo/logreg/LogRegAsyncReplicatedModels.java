package de.tuberlin.pserver.benchmarks.criteo.logreg;

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
import de.tuberlin.pserver.dsl.unit.controlflow.loop.LoopBody;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.LoopTermination;
import de.tuberlin.pserver.runtime.core.config.ConfigLoader;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.util.Arrays;

public class LogRegAsyncReplicatedModels extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_EPOCHS = 50;
    private static final float STEP_SIZE = 0.5f;

    private static final String DATA_PATH = "/criteo/criteo_train";
    private static final long ROWS = 195841983;
    private static final long COLS = 1048615;

    //private static final String DATA_PATH = "datasets/svm_train";
    //private static final long ROWS = 80000;
    //private static final long COLS = 1048615;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = 1)
    public DenseMatrix32F labels;

    @Load(filePath = DATA_PATH, labels = "labels")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS)
    public CSRMatrix32F features;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, remoteUpdates, localState) -> {
                int count = 1;
                if (remoteUpdates != null) {
                    for (final Matrix32F update : remoteUpdates) {
                        Parallel.For(update, (i, j, v) -> W.set(i, j, W.get(i, j) + update.get(i, j)));
                        count++;
                    }
                    W.scale(1.0f / count, W);
                }
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.process(() -> {

            DenseMatrix32F grad = (DenseMatrix32F)new MatrixBuilder().dimension(1, features.cols()).build();
            DenseMatrix32F derivative = (DenseMatrix32F)new MatrixBuilder().dimension(1, features.cols()).build();

            UnitMng.loop(NUM_EPOCHS, epoch -> {

                Arrays.fill(derivative.data, 0f);
                Arrays.fill(grad.data, 0f);
                features.processRows((row, valueList, rowStart, rowEnd, colList) -> {
                    float yPredict = 0;
                    for (int i = rowStart; i < rowEnd; ++i) {
                        yPredict += valueList[i] * W.data[colList[i]];
                    }
                    float f = labels.data[row] - yPredict;
                    for (int j = rowStart; j < rowEnd; ++j) {
                        int ci = colList[j];
                        derivative.data[ci] = valueList[ci] * f;
                        grad.data[ci] += derivative.data[ci] * STEP_SIZE;
                        W.data[ci] -= grad.data[ci];
                    }
                });

                //
                // Model Merging.
                //

                TransactionMng.commit(syncW);
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), LogRegAsyncReplicatedModels.class)
                .done();
    }
}
