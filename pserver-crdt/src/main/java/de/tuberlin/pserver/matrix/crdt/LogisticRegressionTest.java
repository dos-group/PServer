package de.tuberlin.pserver.matrix.crdt;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.ml.optimization.GradientDescent.GDOptimizer;
import de.tuberlin.pserver.ml.optimization.*;

import java.io.Serializable;
import java.util.List;


public class LogisticRegressionTest extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String NUM_NODES = "2";

    private static final String X_TRAIN_PATH = "/Users/Pat/Programming/Java/PServer/datasets/X_train.csv";
    private static final String Y_TRAIN_PATH = "/Users/Pat/Programming/Java/PServer/datasets/Y_train.csv";
    private static final String X_TEST_PATH = "/Users/Pat/Programming/Java/PServer/datasets/X_test.csv";
    private static final String Y_TEST_PATH = "/Users/Pat/Programming/Java/PServer/datasets/Y_test.csv";

    private static final int N_TRAIN = 16;
    private static final int N_TEST = 4;
    private static final int D = 3;

    private static double STEP_SIZE = 1e-3;
    private static int NUM_EPOCHS = 1;
    private static double LAMBDA = 1.0;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH)
    public Matrix64F XTrain;

    @State(scope = Scope.PARTITIONED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix64F yTrain;

    @State(scope = Scope.REPLICATED, rows = N_TEST, cols = D, path = X_TEST_PATH)
    public Matrix64F XTest;

    @State(scope = Scope.REPLICATED, rows = N_TEST, cols = 1, path = Y_TEST_PATH)
    public Matrix64F yTest;

    /*@State(scope = Scope.REPLICATED, rows = 1, cols = D)
    public Matrix64F W;*/

    public ExactAvgDenseMatrix64F W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

   /* @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(

            (Update<Matrix64F>) (remoteUpdates, localState) -> {
                int count = 1;
                for (final Matrix64F update : remoteUpdates) {
                    Parallel.For(update, (i, j, v) -> W.set(i, j, W.get(i, j) + update.get(i, j)));
                    count++;
                }
                W.scale(1. / count, W);
            }
    );*/

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {

        /*lifecycle.process(() -> {
            W = new ExactAvgDenseMatrix64F(1, 3, "weights", Integer.parseInt(NUM_NODES), programContext);

            final Scorer zeroOneLoss = new Scorer(
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

            optimizer.optimize(XTrain, yTrain, W);

            System.out.println("Loss[" + programContext.nodeID +"]: "
                    + zeroOneLoss.score(XTest, yTest, W));
            System.out.println("Accuracy[" + programContext.nodeID +"]: "
                    + accuracy.score(XTest, yTest, W));

        }).postProcess(() -> {

            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);

            //TransactionMng.commit(syncW);

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

            result(W);
        });*/
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
                .run(LogisticRegressionTest.class)
                .results(result)
                .done();

        Matrix model = (Matrix) result.get(0).get(0);
        System.out.println(model);
    }
}
