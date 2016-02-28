package de.tuberlin.pserver.examples.experiments.regression;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.util.Arrays;


public class LogRegCriteoRM extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String X_TRAIN_PATH = "datasets/svm_train";
    private static final int N_TRAIN = 80000;
    private static final int D = 1048615;
    private static float STEP_SIZE = 1e-3f;
    private static int NUM_EPOCHS = 100;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = N_TRAIN, cols = 1)
    public DenseMatrix32F trainLabel;

    @Load(filePath = X_TRAIN_PATH, labels = "trainLabel")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = N_TRAIN, cols = D)
    public CSRMatrix32F trainFeatures;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = D)
    public DenseMatrix32F W;

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.process(() -> {

            /*DenseMatrix32F grad = (DenseMatrix32F)new MatrixBuilder().dimension(1, trainFeatures.cols()).build();
            DenseMatrix32F derivative = (DenseMatrix32F)new MatrixBuilder().dimension(1, trainFeatures.cols()).build();
            for (int e = 0; e < NUM_EPOCHS; ++e) {
                Arrays.fill(derivative.data, 0f);
                Arrays.fill(grad.data, 0f);
                trainFeatures.processRows((row, valueList, rowStart, rowEnd, colList) -> {
                    float yPredict = 0;
                    for (int i = rowStart; i < rowEnd; ++i) {
                        yPredict += valueList[i] * W.data[colList[i]];
                    }
                    float f = trainLabel.data[row] - yPredict;
                    for (int j = rowStart; j < rowEnd; ++j) {
                        int ci = colList[j];
                        derivative.data[ci] = valueList[ci] * f;
                        grad.data[ci] += derivative.data[ci] * STEP_SIZE;
                        W.data[ci] -= grad.data[ci];
                    }
                });
            }*/
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("global.simNodes", "2");

        PServerExecutor.LOCAL
                .run(LogRegCriteoRM.class)
                .done();
    }
}
