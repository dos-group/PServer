package de.tuberlin.pserver.examples.experiments.regression;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.runtime.state.matrix.MatrixBuilder;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class LogisticRegressionCriteoFast extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String NUM_NODES = "1";

    private static final String X_TRAIN_PATH = "datasets/data_train";
    private static final String Y_TRAIN_PATH = "datasets/labels_train";
    private static final String X_TEST_PATH  = "datasets/data_test";
    private static final String Y_TEST_PATH  = "datasets/labels_test";

    private static final int N_TRAIN = 80000;
    private static final int N_TEST = 20000;
    private static final int D = 1048615;

    private static float STEP_SIZE = 1e-3f;
    private static int NUM_EPOCHS = 25;
    private static float LAMBDA = 1.0f;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.REPLICATED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH, matrixFormat = MatrixFormat.SPARSE_FORMAT)
    public Matrix32F XTrain;

    @State(scope = Scope.REPLICATED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix32F yTrain;

    @State(scope = Scope.REPLICATED, rows = 1, cols = D)
    public Matrix32F W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(

            (Update<Matrix32F>) (remoteUpdates, localState) -> {
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


    private static int binarySearch(long[] a, int fromIndex, int toIndex, long key) {

        int low = fromIndex;

        int high = toIndex - 1;

        while (low <= high) {

            int mid = (low + high) >>> 1;

            long midVal = a[mid];

            if (midVal < key)

                low = mid + 1;

            else if (midVal > key)

                high = mid - 1;

            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    int[] rowPtr = new int[2];

    int s = 0;

    private int[] getRowPtr(long[] keys, long row, long cols) {

        final long lastRowIndex = row * cols + cols - 1;

        int o = s;

        final int range = (int)(o + cols > keys.length
                ? keys.length : o + cols);

        s = binarySearch(keys, o, range, lastRowIndex);

        if (s < 0)
            s = (s * -1) - 1;

        rowPtr[0] = o;

        rowPtr[1] = s - o;

        return rowPtr;
    }

    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.process(() -> {

            SparseMatrix32F data = (SparseMatrix32F)XTrain;

            data.createSortedKeys();

            DenseMatrix32F M = (DenseMatrix32F)W;

            DenseMatrix32F Y = (DenseMatrix32F)yTrain;

            DenseMatrix32F grad = new MatrixBuilder().dimension(1, data.cols()).build();

            DenseMatrix32F derivative = new MatrixBuilder().dimension(1, data.cols()).build();

            /*for (int i = 0; i < 100; ++i) {
                int[] rowPtr = getRowPtr(data.sortedKeys, i, data.cols());
                System.out.println(" ====> s = " + rowPtr[0] + " ====> n = " + rowPtr[1]);
            }*/


            // 235778 ms

            // 229348 ms

            // 215658 ms

            final long dataCols = data.cols();

            final long dataRows = data.rows();

            for (int e = 0; e < NUM_EPOCHS; ++e) {

                System.out.println("COMPUTE EPOCH -> " + e);

                for (int i = 0; i < dataRows; ++i) {

                    Arrays.fill(derivative.data, 0f);

                    Arrays.fill(grad.data, 0f);

                    int[] rowPtr = getRowPtr(data.sortedKeys, i, dataCols);

                    float yPredict = 0;

                    for (int j = 0; j < rowPtr[1]; ++j) {

                        yPredict += data.data.get(data.sortedKeys[rowPtr[0] + j]) * M.data[(int)(data.sortedKeys[rowPtr[0] + j] - (i * dataCols))];
                    }

                    float f = Y.data[i] - yPredict;

                    for (int j = 0; j < rowPtr[1]; ++j) {

                        int o = rowPtr[0] + j;

                        int x = (int)(data.sortedKeys[o] - (i * dataCols));

                        derivative.data[x] = data.data.get(data.sortedKeys[o]) * f;

                        grad.data[x] = (grad.data[x] + derivative.data[x]) * STEP_SIZE;

                        M.data[x] = M.data[x] - grad.data[x];
                    }
                }

                s = 0;
            }


            //SparseMatrix32F.SparseMatrix32View view = new SparseMatrix32F.SparseMatrix32View((SparseMatrix32F)XTrain);

            /*final Scorer zeroOneLoss = new Scorer(
                    new ScoreFunction.ZeroOneLoss(),
                    new PredictionFunction.LinearBinaryPrediction());

            final Scorer accuracy = new Scorer(
                    new ScoreFunction.Accuracy(),
                    new PredictionFunction.LinearBinaryPrediction());

            final LossFunction lossFct = new LossFunction.GenericLossFunction(
                    new PredictionFunction.LinearPrediction(),
                    new PartialLossFunction.LogLoss(),
                    new RegularizationFunction.L2Regularization());

            final GDOptimizer optimizer = new GDOptimizer()
                    .setMaxIterations(NUM_EPOCHS)
                    .setBatchSize(1)
                    .setInitialLearningRate(STEP_SIZE)
                    .setLearningRateFunction(new LearningRateFunction.ConstantLearningRate())
                    .setLossFunction(lossFct)
                    .setRegularization(LAMBDA)
                    .setSyncMode(Loop.ASYNCHRONOUS)
                    .setShuffle(false);

            optimizer.optimize(XTrain, yTrain, W);*/

            /*System.out.println("Loss[" + programContext.nodeID +"]: "
                    + zeroOneLoss.score(XTest, yTest, W));
            System.out.println("Accuracy[" + programContext.nodeID +"]: "
                    + accuracy.score(XTest, yTest, W));*/

        }).postProcess(() -> {

            /*UnitMng.barrier(UnitMng.GLOBAL_BARRIER);

            TransactionMng.commit(syncW);

            final Scorer zeroOneLoss = new Scorer(
                    new ScoreFunction.ZeroOneLoss(),
                    new PredictionFunction.LinearBinaryPrediction());

            final Scorer accuracy = new Scorer(
                    new ScoreFunction.Accuracy(),
                    new PredictionFunction.LinearBinaryPrediction());

            System.out.println("Loss merged[" + programContext.nodeID +"]: "
                    + zeroOneLoss.score(XTest, yTest, W));
            System.out.println("Accuracy merged[" + programContext.nodeID +"]: "
                    + accuracy.score(XTest, yTest, W));

            result(W);*/
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("simulation.numNodes", NUM_NODES);

        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(LogisticRegressionCriteoFast.class)
                .done();

        //Matrix model = (Matrix) result.get(0).get(0);
        //System.out.println(model);
    }
}
