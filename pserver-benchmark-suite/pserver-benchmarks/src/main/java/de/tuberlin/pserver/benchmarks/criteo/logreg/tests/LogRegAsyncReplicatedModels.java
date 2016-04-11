package de.tuberlin.pserver.benchmarks.criteo.logreg.tests;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.commons.serialization.LocalFSObjectStorage;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.commons.config.ConfigLoader;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class LogRegAsyncReplicatedModels extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_EPOCHS = 15;
    private static final float STEP_SIZE = 1.0f;

    private static final String DATA_PATH = "/criteo/criteo_train";
    private static final long ROWS = 195841983;
    private static final long COLS = 1048615;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int txnCounter = 0;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = 1)
    public DenseMatrix32F labels;

    @Load(filePath = DATA_PATH, labels = "Y")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS)
    public CSRMatrix32F features;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition merge_W = new TransactionDefinition(

        (Update<Matrix32F>) (requestObj, remoteUpdates, localState) -> {
            if (remoteUpdates != null) {
                for (final Matrix32F update : remoteUpdates) {
                    Parallel.For(update, (i, j, v) -> W.set(i, j, W.get(i, j) + update.get(i, j)));
                }
                W.scale(1.0f / remoteUpdates.size(), W);
                System.out.println("MODEL_MERGE_TRANSACTION [" + (txnCounter++) + "]");
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

                //
                // Local Model Update.
                //

                Arrays.fill(derivative.data, 0f);
                Arrays.fill(grad.data, 0f);

                atomic(state(W), () ->
                    features.processRows((id, row, valueList, rowStart, rowEnd, colList) -> {

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
                    })
                );

                //
                // Global Model Merging.
                //

                TransactionMng.commit(merge_W);

                LocalFSObjectStorage.writeTo(W, "/home/tobias.herb/criteo_model_" + epoch);

                System.out.println("FINISHED EPOCH [" + epoch + "]");
            });

        }).postProcess(() -> {

            if (programContext.node(0)) {

                TransactionMng.commit(merge_W);

                System.out.println("- MERGED FINAL MODELS -");

                result(W);
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), LogRegAsyncReplicatedModels.class)
                .results(result)
                .done();
    }
}
