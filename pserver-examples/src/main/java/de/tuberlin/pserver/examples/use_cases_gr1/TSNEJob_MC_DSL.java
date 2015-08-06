package de.tuberlin.pserver.examples.use_cases_gr1;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.cf.Iteration;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.playground.exp1.tuples.Tuple2;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;


public class TSNEJob_MC_DSL extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final boolean DEBUG = true;

    private final String INPUT_MATRIX = "/Users/Chris/Downloads/mnist_10_X.csv";
    private final String Y_INIT_MATRIX = "/Users/Chris/Downloads/mnist_10_initY.csv";
    private final String OUTPUT_MATRIX = "/Users/Chris/Downloads/pserver_mnist_result.csv";

    private final Integer INPUT_ROWS = 10;
    private final Integer INPUT_COLS = 28*28;
    private final Integer EMBEDDING_DIMENSION = 2;

    // Algorithm parameters

    private final int NUM_EPOCHS = 200;
    private final double PERPLEXITY = 2.0;
    private final double LEARNING_RATE = 350.0;
    private final double EARLY_EXAGGERATION = 1.0;

    private final double INITIAL_MOMENTUM = 0.5;
    private final double FINAL_MOMENTUM = 0.8;
    private final double MIN_GAIN = 0.01;
    private final double TOL = 1e-5;

    private Matrix X;
    private Matrix P;
    private Matrix Q;
    private Matrix Y;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadAsMatrix(
                INPUT_MATRIX,
                INPUT_ROWS,
                INPUT_COLS,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD)
        );

        // if debugging is enabled, use a fixed initialization for Y
        if (DEBUG) {
            dataManager.loadAsMatrix(
                    Y_INIT_MATRIX,
                    INPUT_ROWS,
                    EMBEDDING_DIMENSION,
                    RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD)
            );
        } else {
            Y = new MatrixBuilder()
                .dimension(INPUT_ROWS, EMBEDDING_DIMENSION)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

            final Random rand = new Random();
            // initialize Y by sampling from N(0, 1)
            Y.applyOnElements(e -> e = rand.nextDouble());
            dataManager.putObject("Y", Y);
        }
    }

    @Override
    public void compute() {
        X = dataManager.getObject(INPUT_MATRIX);

        if (DEBUG) {
            Y = dataManager.getObject(Y_INIT_MATRIX);
        } else {
            Y = dataManager.getObject("Y");
        }

        P = binarySearch(X, TOL, PERPLEXITY);
        P = P.add(P.transpose());

        // ---------------------------------------------------
        // (1) GLOBAL OPERATION!!!
        // ---------------------------------------------------
        double sumP = P.aggregateRows(f -> f.sum()).sum();

        P = P.scale(1 / sumP);

        // early exaggeration
        P = P.scale(EARLY_EXAGGERATION);

        final long n = Y.numRows();
        final long d = Y.numCols();

        Q = new MatrixBuilder()
                .dimension(n, n)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Matrix Y_squared = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Matrix gains = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Matrix iY = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Matrix dY = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Vector mean = new VectorBuilder()
                .dimension(d)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.ROW_LAYOUT)
                .build();

        gains.assign(1.0);
        iY.assign(0.0);

        final MutableDouble momentum = new MutableDouble(0.0);

        CF.iterate()
                .sync(Iteration.GLOBAL | Iteration.LOCAL)
                .execute(NUM_EPOCHS, (epoch) -> {

                    Y_squared.assign( Y_squared.applyOnElements(Y, e -> Math.pow(e, 2)) );

                    final Vector sum_Y = Y_squared.aggregateRows(f -> f.sum());

                    final Matrix num = Y.mul( Y.transpose() )
                            .scale(-2)
                            .addVectorToRows(sum_Y)
                            .transpose()
                            .addVectorToRows(sum_Y)
                            .applyOnElements(e -> 1.0 / (e + 1.0));

                    num.assign(num.zeroDiagonal());

                    final double sumNum = num.aggregateRows(f -> f.sum()).sum();

                    Q.assign(num.copy().scale(1.0 / sumNum));

                    Q.assign(Q.applyOnElements(e -> Math.max(e, 1e-12)));

                    final Matrix PQ = P.copy().sub(Q);

                    CF.iterate()
                            .execute(P.numRows(), (i) -> {

                                final Vector sumVec = new VectorBuilder()
                                        .dimension(d)
                                        .format(Vector.Format.DENSE_VECTOR)
                                        .layout(Vector.Layout.ROW_LAYOUT)
                                        .build();

                                CF.iterate()
                                        .execute(P.numCols(), (j) -> {

                                            final Double pq = PQ.get(i, j);
                                            final Double num_j = num.get(i, j);

                                            sumVec.assign(
                                                    sumVec.add(
                                                            Y.rowAsVector(i)
                                                                    .sub(Y.rowAsVector(j))
                                                                    .applyOnElements(e -> e * pq * num_j)
                                                    )
                                            );

                                        });

                                dY.assignRow(i, sumVec);
                            });

                    momentum.setValue( (epoch < 20) ? INITIAL_MOMENTUM : FINAL_MOMENTUM);

                    CF.iterate()
                            .execute(gains, (i, j, v) -> {

                                final Double dY_j = dY.get(i, j);
                                final Double iY_j = iY.get(i, j);
                                final Double gain_j = gains.get(i, j);

                                gains.set(i, j, (dY_j > 0) == (iY_j > 0) ? gain_j * 0.8 : gain_j + 0.2);
                            });

                    gains.assign(gains.applyOnElements(e -> Math.max(e, MIN_GAIN)));

                    iY.assign(
                            iY.scale(momentum.getValue())
                                    .sub(dY.applyOnElements(gains, (e1, e2) -> e1 * e2).scale(LEARNING_RATE))
                    );

                    Y.assign(Y.add(iY));

                    mean.assign(0.0);

                    for (int i = 0; i < Y.numRows(); ++i)
                        mean.add(Y.rowAsVector(i));

                    mean.assign(mean.div(Y.numRows()));

                    Y.assign(Y.addVectorToRows(mean.mul(-1.0)));

                    if ((epoch + 1) % 10 == 0) {

                        double C = 0.0;
                        for (int i = 0; i < P.numRows(); ++i) {
                            for (int j = 0; j < P.numCols(); ++j) {
                                if (i != j) {
                                    C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                                }
                            }
                        }

                        LOG.info("Iteration " + (epoch + 1) + ", Error: " + C);
                    }

                    if (epoch == 100) {
                        P.assign(P.scale(1.0 / EARLY_EXAGGERATION));
                    }

                    LOG.debug("Y: " + Y.rowAsVector().toString());
                });

        result(Y);
    }

    private Matrix binarySearch(Matrix X, Double tol, Double perplexity) {
        long n = X.numRows();
        long d = X.numCols();

        Matrix Xsquared = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Matrix P = new MatrixBuilder()
                .dimension(n, n)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Vector beta = new VectorBuilder()
                .dimension(n)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.ROW_LAYOUT)
                .build();

        P.assign(0.0);
        beta.assign(1.0);

        // compute the distances between all x_i
        Xsquared = Xsquared.applyOnElements(X, e -> Math.pow(e, 2));
        Vector sumX = Xsquared.aggregateRows(f -> f.sum());

        Matrix D = X.mul(X.transpose()).scale(-2).addVectorToRows(sumX).transpose()
                .addVectorToRows(sumX);

        double logU = Math.log(perplexity);

        for (long i=0; i < n; ++i) {

            double betaMin = Double.NEGATIVE_INFINITY;
            double betaMax = Double.POSITIVE_INFINITY;

            Vector Di = D.rowAsVector(i);

            Tuple2<Double, Vector> hBeta = HBeta(Di, beta.get(i), i);

            double H = hBeta._1;
            Vector Pi = hBeta._2;

            // Evaluate whether the perplexity is within tolerance
            double HDiff = H - logU;
            int tries = 0;
            while(Math.abs(HDiff) > tol && tries < 50){
                if (HDiff > 0){
                    betaMin = beta.get(i);
                    if (Double.isInfinite(betaMax))
                        beta.set(i, beta.get(i) * 2);
                    else
                        beta.set(i, (beta.get(i) + betaMax) / 2);
                } else{
                    betaMax = beta.get(i);
                    if (Double.isInfinite(betaMin))
                        beta.set(i, beta.get(i) / 2);
                    else
                        beta.set(i, (beta.get(i) + betaMin) / 2);
                }

                hBeta = HBeta(Di, beta.get(i), i);
                H = hBeta._1;
                Pi = hBeta._2;
                HDiff = H - logU;
                tries = tries + 1;
            }
            P.assignRow(i, Pi);
        }
        return P;
    }

    private Tuple2<Double, Vector> HBeta (Vector d, Double beta, Long index){
        Vector P = new VectorBuilder()
                .dimension(d.length())
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.ROW_LAYOUT)
                .build();

        P.set(index, 0.0);

        // executePartitioned over all elements i != j
        for (long i=0; i < P.length(); ++i) {
            if (i != index) {
                P.set(i, Math.exp(-1 * d.get(i) * beta));
            }
        }

        Vector PD = P.copy().applyOnElements(d, (e1, e2) -> e1 * e2);

        double sumP = P.sum();
        double H = Math.log(sumP) + beta * PD.sum() / sumP;
        P = P.applyOnElements(e -> e / sumP);

        return new Tuple2<>(H, P);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();
        PrintWriter writer = null;

        PServerExecutor.LOCAL
                .run(TSNEJob_MC_DSL.class)
                .results(res)
                .done();

        try {
            writer = new PrintWriter("/Users/Chris/Downloads/pserver_mnist.csv", "UTF-8");
            final DecimalFormat numberFormat = new DecimalFormat("0.000");
            Matrix R = (Matrix) res.get(0).get(0);
            for (int i = 0; i < R.numRows(); ++i) {
                for (int j = 0; j < R.numCols(); ++j) {
                    writer.println(i + "," + j + "," + R.get(i, j));
                }
            }
        }
        catch (Exception e) {}
        finally {
            writer.close();
        }
    }
}