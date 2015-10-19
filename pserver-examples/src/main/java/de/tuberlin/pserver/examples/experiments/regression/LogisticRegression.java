package de.tuberlin.pserver.examples.experiments.regression;

import com.google.common.collect.Lists;
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
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.GradientDescent.GDOptimizer;
import de.tuberlin.pserver.runtime.parallel.Parallel;

import java.io.Serializable;
import java.util.List;


public class LogisticRegression extends Program {

    private static final String NUM_NODES = "2";

    private static final String X_TRAIN_PATH = "/Users/Chris/Downloads/X_train.csv";
    private static final String Y_TRAIN_PATH = "/Users/Chris/Downloads/Y_train.csv";
    private static final String X_TEST_PATH = "/Users/Chris/Downloads/X_test.csv";
    private static final String Y_TEST_PATH = "/Users/Chris/Downloads/Y_test.csv";

    private static final int N_TRAIN = 16;
    private static final int N_TEST = 4;
    private static final int D = 3;

    private static double STEP_SIZE = 1e-3;
    private static int NUM_EPOCHS = 1;
    private static double LAMBDA = 1.0;


    @State(scope = Scope.PARTITIONED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH)
    public Matrix64F XTrain;

    @State(scope = Scope.PARTITIONED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix64F yTrain;

    @State(scope = Scope.REPLICATED, rows = N_TEST, cols = D, path = X_TEST_PATH)
    public Matrix64F XTest;

    @State(scope = Scope.REPLICATED, rows = N_TEST, cols = 1, path = Y_TEST_PATH)
    public Matrix64F yTest;

    @State(scope = Scope.REPLICATED, rows = 1, cols = D)
    public Matrix64F W;


    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(

            (Apply<Matrix64F, Void>) (updates) -> {
                int count = 1;
                for (final Matrix64F update : updates) {
                    Parallel.For(update, (i, j, v) -> W.set(i, j, W.get(i, j) + update.get(i, j)));
                    count++;
                }
                W.scale(1. / count, W);
                return null;
            }
    );


    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

        }).process(() -> {

            Scorer zeroOneLoss = new Scorer(
                    new ScoreFunction.ZeroOneLoss(),
                    new PredictionFunction.LinearBinaryPrediction());

            Scorer accuracy = new Scorer(
                    new ScoreFunction.Accuracy(),
                    new PredictionFunction.LinearBinaryPrediction());

            LossFunction lossFct = new LossFunction.GenericLossFunction(
                    new PredictionFunction.LinearPrediction(),
                    new PartialLossFunction.LogLoss(),
                    new RegularizationFunction.L2Regularization());

            GDOptimizer optimizer = new GDOptimizer()
                    .setMaxIterations(NUM_EPOCHS)
                    .setBatchSize(1)
                    .setInitialLearningRate(STEP_SIZE)
                    .setLearningRateFunction(new LearningRateFunction.ConstantLearningRate())
                    .setLossFunction(lossFct)
                    .setRegularization(LAMBDA)
                    .setSyncMode(Loop.ASYNCHRONOUS)
                    .setShuffle(false);

            optimizer.optimize(XTrain, yTrain, W);

            System.out.println("Loss[" + programContext.runtimeContext.nodeID +"]: "
                    + zeroOneLoss.score(XTest, yTest, W));
            System.out.println("Accuracy[" + programContext.runtimeContext.nodeID +"]: "
                    + accuracy.score(XTest, yTest, W));

        }).postProcess(() -> {
            UnitMng.barrier(UnitMng.GLOBAL_BARRIER);

            TransactionMng.commit(syncW);

            Scorer zeroOneLoss = new Scorer(
                    new ScoreFunction.ZeroOneLoss(),
                    new PredictionFunction.LinearBinaryPrediction());

            Scorer accuracy = new Scorer(
                    new ScoreFunction.Accuracy(),
                    new PredictionFunction.LinearBinaryPrediction());

            System.out.println("Loss merged[" + programContext.runtimeContext.nodeID +"]: "
                    + zeroOneLoss.score(XTest, yTest, W));
            System.out.println("Accuracy merged[" + programContext.runtimeContext.nodeID +"]: "
                    + accuracy.score(XTest, yTest, W));

            result(W);
        });
    }


    public static void local() {
        System.setProperty("simulation.numNodes", NUM_NODES);

        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(LogisticRegression.class)
                .results(result)
                .done();

        Matrix model = (Matrix) result.get(0).get(0);
        System.out.println(model);
    }

    public static void main(final String[] args) {
        local();
    }
}
