package de.tuberlin.pserver.examples.experiments.regression;


import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix.RowIterator;
import de.tuberlin.pserver.runtime.MLProgram;

import java.io.Serializable;
import java.util.List;

public class LogisticRegression extends MLProgram {

    private static final String NUM_NODES = "1";
    private static final int PER_NODE_PARALLELISM = 1;

    /*
    private static final String X_TRAIN_PATH = "/Users/Chris/Downloads/X_train.csv";
    private static final String Y_TRAIN_PATH = "/Users/Chris/Downloads/Y_train.csv";
    private static final String X_TEST_PATH = "/Users/Chris/Downloads/X_test.csv";
    private static final String Y_TEST_PATH = "/Users/Chris/Downloads/Y_test.csv";

    private static final int N_TRAIN = 160;
    private static final int N_TEST = 40;
    private static final int D = 3;

    */

    private static final String X_TRAIN_PATH = "/Users/Chris/Downloads/X_house_train.csv";
    private static final String Y_TRAIN_PATH = "/Users/Chris/Downloads/Y_house_train.csv";
    private static final String X_TEST_PATH = "/Users/Chris/Downloads/X_house_test.csv";
    private static final String Y_TEST_PATH = "/Users/Chris/Downloads/Y_house_test.csv";

    private static final int N_TRAIN = 538;
    private static final int N_TEST = 135;
    private static final int D = 2;


    private static double STEP_SIZE = 1e-2;
    private static int NUM_EPOCHS = 20;
    private static double LAMBDA = 1e-6;
    private static int PERIOD = 1;


    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TRAIN, cols = D, path = X_TRAIN_PATH)
    public Matrix X_train;

    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TRAIN, cols = 1, path = Y_TRAIN_PATH)
    public Matrix Y_train;

    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TEST, cols = D, path = X_TEST_PATH)
    public Matrix X_test;

    @State(globalScope = GlobalScope.PARTITIONED, rows = N_TEST, cols = 1, path = Y_TEST_PATH)
    public Matrix Y_test;

    @State(globalScope = GlobalScope.REPLICATED, rows = 1, cols = D)
    public Matrix W;

    @Unit
    public void main(Program program) {

        program.initialize(() -> {

        }).process(() -> {

            final RowIterator trainIterator = X_train.rowIterator();
            final RowIterator testIterator = X_test.rowIterator();

            CF.loop().exe(NUM_EPOCHS, (epoch) -> {
                int i = 0;
                while (trainIterator.hasNext()) {
                    trainIterator.nextRandom();
                    final Matrix xi = trainIterator.get();
                    final double yi = Y_train.get(trainIterator.rowNum());

                    //updateW(xi, W, yi, STEP_SIZE);
                    updateWReg(xi, W, yi, STEP_SIZE, LAMBDA);
                }
                trainIterator.reset();

                if (epoch % PERIOD == 0) {
                    System.out.println("Objective[" + epoch + "]: " + likelihoodReg(trainIterator, Y_train, W, LAMBDA));
                    System.out.println("Loss[" + epoch + "]: " + zeroOneLoss(testIterator, Y_test, W));
                }
            });

        }).postProcess(() -> {
            result(W);
        });
    }

    public static double sigmoid(double z) {
        return 1. / (1. + Math.exp(-z));
    }

    public static void updateW(Matrix x, Matrix W, double y, double stepSize) {
        W.add(x.scale(stepSize * y * sigmoid(-y * x.dot(W))), W);
    }

    public static double likelihood(RowIterator dataIterator, Matrix Y, Matrix W) {
        double sum = 0.0;

        while (dataIterator.hasNext()) {
            dataIterator.next();
            final Matrix xi = dataIterator.get();
            final double yi = Y.get(dataIterator.rowNum());

            double zi = xi.dot(W);
            sum += Math.log(sigmoid(yi * zi));
        }
        dataIterator.reset();
        return sum;
    }

    public static void updateWReg(Matrix x, Matrix W, double y, double stepSize, double lambda) {
        Matrix Wnew = W.add(x.scale(stepSize * y * sigmoid(-y * x.dot(W))));
        W.set(0,0,0.0);
        Wnew.sub(W.scale(stepSize*lambda), W);
    }

    public static double likelihoodReg(RowIterator dataIterator, Matrix Y, Matrix W, double lambda) {
        double sum = 0.0;

        while (dataIterator.hasNext()) {
            dataIterator.next();
            final Matrix xi = dataIterator.get();
            final double yi = Y.get(dataIterator.rowNum());

            double zi = xi.dot(W);
            sum += Math.log(sigmoid(yi * zi));
        }
        sum -= 0.5 * lambda * (W.applyOnElements(e -> Math.pow(e, 2)).sum() - Math.pow(W.get(0), 2));

        dataIterator.reset();
        return sum;
    }

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
