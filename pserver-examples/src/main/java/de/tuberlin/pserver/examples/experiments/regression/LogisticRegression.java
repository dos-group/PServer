package de.tuberlin.pserver.examples.experiments.regression;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.GradientDescent.GDOptimizer;
import de.tuberlin.pserver.runtime.MLProgram;

import java.io.Serializable;
import java.util.List;

public class LogisticRegression extends MLProgram {

    private static final String NUM_NODES = "1";
    private static final int PER_NODE_PARALLELISM = 1;

    private static final String X_TRAIN_PATH = "/Users/Chris/Downloads/X_train.csv";
    private static final String Y_TRAIN_PATH = "/Users/Chris/Downloads/Y_train.csv";
    private static final String X_TEST_PATH = "/Users/Chris/Downloads/X_test.csv";
    private static final String Y_TEST_PATH = "/Users/Chris/Downloads/Y_test.csv";

    private static final int N_TRAIN = 3200;
    private static final int N_TEST = 800;
    private static final int D = 3;

    private static double STEP_SIZE = 1e-3;
    private static int NUM_EPOCHS = 1000;
    private static double LAMBDA = 1.0;
    private static int PERIOD = 1;


    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH)
    public Matrix X_train;

    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix y_train;

    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TEST, cols = D, path = X_TEST_PATH)
    public Matrix X_test;

    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TEST, cols = 1, path = Y_TEST_PATH)
    public Matrix y_test;

    @State(globalScope = GlobalScope.REPLICATED, rows = 1, cols = D)
    public Matrix W;

    @Unit
    public void main(Program program) {

        program.initialize(() -> {

        }).process(() -> {

            LossFunction lossFct = new LossFunction.GenericLossFunction(
                    new PredictionFunction.LinearPrediction(),
                    new PartialLossFunction.LogLoss(),
                    new RegularizationFunction.L2Regularization()
            );

            GDOptimizer optimizer = new GDOptimizer(slotContext)
                    .setMaxIterations(NUM_EPOCHS)
                    .setBatchSize(1)
                    .setInitialLearningRate(STEP_SIZE)
                    .setLearningRateFunction(new LearningRateFunction.ConstantLearningRate())
                    .setLossFunction(lossFct)
                    .setRegularization(LAMBDA)
                    .setShuffle(false);

            optimizer.optimize(X_train, y_train, W);

        }).postProcess(() -> {
            result(W);
        });
    }

    /*
    public static double zeroOneLoss(RowIterator dataIterator, Matrix Y, Matrix W) {
        double loss = 0.0;

        while (dataIterator.hasNext()) {
            dataIterator.next();
            final Matrix xi = dataIterator.get();
            final double yi = Y.get(dataIterator.rowNum());

            double zi = xi.dot(W);

            double prediction = Math.signum(sigmoid(zi) - 0.5);

            if (prediction != yi) {
                loss++;
            }
        }
        dataIterator.reset();
        return loss;
    }
    */


    public static void local() {
        System.setProperty("simulation.numNodes", NUM_NODES);

        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(LogisticRegression.class, PER_NODE_PARALLELISM)
                .results(result)
                .done();

        Matrix model = (Matrix) result.get(0).get(0);
        System.out.println(model);
    }

    public static void main(final String[] args) {
        local();
    }
}
