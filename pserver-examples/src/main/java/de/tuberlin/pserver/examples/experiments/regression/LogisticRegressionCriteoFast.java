package de.tuberlin.pserver.examples.experiments.regression;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.runtime.state.matrix.MatrixBuilder;

import java.util.Arrays;


public class LogisticRegressionCriteoFast extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String NUM_NODES = "1";
    private static final String X_TRAIN_PATH = "datasets/svm_train";
    private static final int N_TRAIN = 80000;
    private static final int D = 1048615;
    private static float STEP_SIZE = 1e-3f;
    private static int NUM_EPOCHS = 1000;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.REPLICATED, rows = N_TRAIN, cols = 1)
    public Matrix32F trainLabel;

    @State(scope = Scope.REPLICATED, rows = N_TRAIN, cols = D,
            path = X_TRAIN_PATH, matrixFormat = MatrixFormat.SPARSE_FORMAT,
            fileFormat = FileFormat.SVM_FORMAT, labels = "trainLabel")

    public Matrix32F trainFeatures;

    @State(scope = Scope.REPLICATED, rows = 1, cols = D)
    public Matrix32F W;

    private CSRMatrix32F csrData = new CSRMatrix32F(D);

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

            csrData = CSRMatrix32F.fromSparseMatrix32F((SparseMatrix32F)trainFeatures);

        }).process(() -> {

            DenseMatrix32F M = (DenseMatrix32F)W;
            DenseMatrix32F Y = (DenseMatrix32F)trainLabel;
            DenseMatrix32F grad = new MatrixBuilder().dimension(1, csrData.cols()).build();
            DenseMatrix32F derivative = new MatrixBuilder().dimension(1, csrData.cols()).build();

            for (int e = 0; e < NUM_EPOCHS; ++e) {
                Arrays.fill(derivative.data, 0f);
                Arrays.fill(grad.data, 0f);
                csrData.processRows((row, valueList, rowStart, rowEnd, colList) -> {
                    float yPredict = 0;
                    for (int i = rowStart; i < rowEnd; ++i) {
                        yPredict += valueList[i] *  M.data[colList[i]];
                    }
                    float f = Y.data[row] - yPredict;
                    for (int j = rowStart; j < rowEnd; ++j) {
                        int ci = colList[j];
                        derivative.data[ci] = valueList[ci] * f;
                        grad.data[ci] += derivative.data[ci] * STEP_SIZE;
                        M.data[ci] -= grad.data[ci];
                    }
                });
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("simulation.numNodes", NUM_NODES);

        PServerExecutor.LOCAL
                .run(LogisticRegressionCriteoFast.class)
                .done();
    }
}
