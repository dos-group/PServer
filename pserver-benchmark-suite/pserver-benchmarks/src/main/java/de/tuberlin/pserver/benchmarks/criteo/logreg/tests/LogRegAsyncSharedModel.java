package de.tuberlin.pserver.benchmarks.criteo.logreg.tests;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Combine;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.commons.config.ConfigLoader;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public class LogRegAsyncSharedModel extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_EPOCHS = 50;
    private static final float STEP_SIZE = 1f;

    private static final String DATA_PATH = "/criteo/criteo_train";
    private static final long ROWS = 195841983;
    private static final long COLS = 1048615;

    //private static final String DATA_PATH = "datasets/svm_train";
    //private static final long ROWS = 80000;
    //private static final long COLS = 1048615;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(at = "0", scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F shared_W;

    // ---------------------------------------------------

    @Matrix(at = "1 - 15", scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = 1)
    public DenseMatrix32F labels;

    @Load(filePath = DATA_PATH, labels = "Y")
    @Matrix(at = "1 - 15", scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS)
    public CSRMatrix32F features;

    @Matrix(at = "1 - 15", scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F cached_W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(src = "shared_W", dst = "cached_W", type = TransactionType.PULL)
    public final TransactionDefinition pull = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, shared_W, cached_W) -> {
                cached_W.assign(shared_W.get(0));
            }
    );

    @Transaction(src = "cached_W", dst = "shared_W", type = TransactionType.PUSH)
    public final TransactionDefinition push = new TransactionDefinition(

            (Combine<Matrix32F>) (requestObj, localModels) -> {
                Matrix32F combinedModel = localModels.get(0);
                for (int i = 1; i < localModels.size(); ++i)
                    combinedModel.add(localModels.get(i), combinedModel);
                combinedModel.scale(1.0f / localModels.size());
                return combinedModel;
            },

            (Update<Matrix32F>) (requestObj, localModels, sharedModel) -> {
                sharedModel.add(localModels.get(0), sharedModel).scale(1.0f / 2);
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit(at = "1 - 15")
    public void unit(Lifecycle lifecycle) {

        lifecycle.process(() -> {

            DenseMatrix32F grad = (DenseMatrix32F)new MatrixBuilder().dimension(1, features.cols()).build();
            DenseMatrix32F derivative = (DenseMatrix32F)new MatrixBuilder().dimension(1, features.cols()).build();

            for (int e = 0; e < NUM_EPOCHS; ++e) {

                //
                // Pull from Parameter Server.
                //

                TransactionMng.commit(pull);

                //
                // Local Model Training.
                //

                /*Arrays.fill(derivative.data, 0f);
                Arrays.fill(grad.data, 0f);
                features.processRows((row, valueList, rowStart, rowEnd, colList) -> {
                    float yPredict = 0;
                    for (int i = rowStart; i < rowEnd; ++i) {
                        yPredict += valueList[i] * cached_W.data[colList[i]];
                    }
                    float f = Y.data[row] - yPredict;
                    for (int j = rowStart; j < rowEnd; ++j) {
                        int ci = colList[j];
                        derivative.data[ci] = valueList[ci] * f;
                        grad.data[ci] += derivative.data[ci] * STEP_SIZE;
                        cached_W.data[ci] -= grad.data[ci];
                    }
                });*/

                //
                // Push to Parameter Server.
                //

                TransactionMng.commit(push);
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), LogRegAsyncSharedModel.class)
                .done();
    }
}