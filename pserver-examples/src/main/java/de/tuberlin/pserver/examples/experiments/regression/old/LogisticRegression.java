package de.tuberlin.pserver.examples.experiments.regression.old;

import com.google.common.collect.Lists;
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
import de.tuberlin.pserver.ml.optimization.GradientDescent.GDOptimizer;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.types.common.FileFormat;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.metadata.DistScheme;

import java.io.Serializable;
import java.util.List;


public class LogisticRegression extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String NUM_NODES = "2";

    private static final String X_TRAIN_PATH = "datasets/X_train.csv";
    private static final String Y_TRAIN_PATH = "datasets/Y_train.csv";
    private static final String X_TEST_PATH  = "datasets/X_test.csv";
    private static final String Y_TEST_PATH  = "datasets/Y_test.csv";

    private static final int N_TRAIN = 16000;
    private static final int N_TEST = 4000;
    private static final int D = 3;

    private static float STEP_SIZE = 1e-3f;
    private static int NUM_EPOCHS = 1;
    private static float LAMBDA = 1.0f;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH, format = FileFormat.SVM_FORMAT)
    public Matrix32F XTrain;

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix32F yTrain;

    @Matrix(scheme = DistScheme.REPLICATED, rows = N_TEST, cols = D, path = X_TEST_PATH)
    public Matrix32F XTest;

    @Matrix(scheme = DistScheme.REPLICATED, rows = N_TEST, cols = 1, path = Y_TEST_PATH)
    public Matrix32F yTest;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = D)
    public Matrix32F W;

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

            result(W);
        });
    }

    // ---------------------------------------------------
    // EntryImpl Point.
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("simulation.numNodes", NUM_NODES);

        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(LogisticRegression.class)
                .results(result)
                .done();

        //Matrix model = (Matrix) result.get(0).get(0);
        //System.out.println(model);
    }
}
