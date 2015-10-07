package de.tuberlin.pserver.examples.experiments.regression;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix.RowIterator;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.GradientDescent.GDOptimizer;

import java.io.Serializable;
import java.util.List;


public class LogisticRegression extends Program {

    private static final String NUM_NODES = "1";

    private static final String X_TRAIN_PATH = "/Users/Chris/Downloads/X_train.csv";
    private static final String Y_TRAIN_PATH = "/Users/Chris/Downloads/Y_train.csv";
    private static final String X_TEST_PATH = "/Users/Chris/Downloads/X_test.csv";
    private static final String Y_TEST_PATH = "/Users/Chris/Downloads/Y_test.csv";

    private static final int N_TRAIN = 16000;
    private static final int N_TEST = 4000;
    private static final int D = 3;

    private static double STEP_SIZE = 1e-3;
    private static int NUM_EPOCHS = 100;
    private static double LAMBDA = 1.0;


    @State(scope = Scope.PARTITIONED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH)
    public Matrix X_train;

    @State(scope = Scope.PARTITIONED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix y_train;

    @State(scope = Scope.PARTITIONED, rows = N_TEST, cols = D, path = X_TEST_PATH)
    public Matrix X_test;

    @State(scope = Scope.PARTITIONED, rows = N_TEST, cols = 1, path = Y_TEST_PATH)
    public Matrix y_test;

    @State(scope = Scope.REPLICATED, rows = 1, cols = D)
    public Matrix W;

    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

        }).process(() -> {

            LossFunction lossFct = new LossFunction.GenericLossFunction(
                    new PredictionFunction.LinearPrediction(),
                    new PartialLossFunction.LogLoss(),
                    new RegularizationFunction.L2Regularization()
            );

            GDOptimizer optimizer = new GDOptimizer()
                    .setMaxIterations(NUM_EPOCHS)
                    .setBatchSize(1)
                    .setInitialLearningRate(STEP_SIZE)
                    .setLearningRateFunction(new LearningRateFunction.ConstantLearningRate())
                    .setLossFunction(lossFct)
                    .setRegularization(LAMBDA)
                    .setShuffle(false);

            optimizer.optimize(X_train, y_train, W);

            System.out.println("Loss: " + zeroOneLoss(X_test.rowIterator(), y_test, W));

        }).postProcess(() -> {
            result(W);
        });
    }


    public static double zeroOneLoss(RowIterator dataIterator, Matrix Y, Matrix W) {
        double loss = 0.0;

        while (dataIterator.hasNext()) {
            dataIterator.next();
            final Matrix xi = dataIterator.get();
            final double yi = Y.get(dataIterator.rowNum());

            double zi = xi.dot(W);

            double prediction = Math.signum(zi);

            if (prediction != yi) {
                loss++;
            }
        }
        dataIterator.reset();
        return loss;
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
