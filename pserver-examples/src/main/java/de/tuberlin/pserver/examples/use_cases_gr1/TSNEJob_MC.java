package de.tuberlin.pserver.examples.use_cases_gr1;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.VectorBuilder;
import de.tuberlin.pserver.playground.exp1.tuples.Tuple2;

import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;


public class TSNEJob_MC extends PServerJob {

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

    private final int MAX_ITER = 200;
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

        dataManager.loadAsMatrix(INPUT_MATRIX, INPUT_ROWS, INPUT_COLS,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        // if debugging is enabled, use a fixed initialization for Y
        if (DEBUG) {
            dataManager.loadAsMatrix(Y_INIT_MATRIX, INPUT_ROWS, EMBEDDING_DIMENSION,
                    RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));
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

        Long n = Y.numRows();
        Long d = Y.numCols();

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

        Matrix dY = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Vector mean = new VectorBuilder()
                .dimension(d)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.ROW_LAYOUT)
                .build();

        gains.assign(1.0);
        iY.assign(0.0);

        double momentum;

        for (int iteration = 0; iteration < MAX_ITER; ++iteration) {
            // sum_Y = Math.sum(Math.square(Y), 1)
            Y_squared = Y_squared.applyOnElements(e -> Math.pow(e, 2), Y);
            Vector sum_Y = Y_squared.aggregateRows(f -> f.sum());

            // num = 1 / (1 + Math.add(Math.add(-2 * Math.dot(Y, Y.T), sum_Y).T, sum_Y))
            Matrix num = Y.mul(Y.transpose()).scale(-2).addVectorToRows(sum_Y).transpose()
                    .addVectorToRows(sum_Y);
            num.applyOnElements(e -> 1.0 / (e + 1.0));
            // entries i=j must be zero
            num = num.setDiagonalsToZero();

            Double sumNum = num.aggregateRows(f -> f.sum()).sum();
            // Q = num / Math.sum(num)
            Q = num.copy().scale(1.0 / sumNum);

            // Math.maximum(Q, 1e-12)
            Q = Q.applyOnElements(e -> Math.max(e, 1e-12));

            // PQ = P - Q
            Matrix PQ = P.copy().sub(Q);

            // dY[i,:] = Math.sum(Math.tile(PQ[:,i] * num[:,i], (no_dims, 1)).T * (Y[i,:] - Y), 0)
            for (int i=0; i < P.numRows(); ++i) {
                Vector sumVec = new VectorBuilder()
                        .dimension(d)
                        .format(Vector.Format.DENSE_VECTOR)
                        .layout(Vector.Layout.ROW_LAYOUT)
                        .build();
                sumVec.assign(0.0);

                for (int j=0; j < P.numCols(); ++j) {
                    final Double pq = PQ.get(i, j);
                    final Double num_j = num.get(i, j);
                    // (p - num / sum(q)) * num
                    // (p - q) * num
                    sumVec = sumVec.add(Y.rowAsVector(i).sub(Y.rowAsVector(j))
                                .applyOnElements(e -> e * pq * num_j));
                }
                dY.assignRow(i, sumVec);
            }

            if (iteration < 20) {
                momentum = INITIAL_MOMENTUM;
            } else {
                momentum = FINAL_MOMENTUM;
            }

            for (int i=0; i < gains.numRows(); ++i) {
                for (int j=0; j < gains.numCols(); ++j) {
                    final Double dY_j = dY.get(i, j);
                    final Double iY_j = iY.get(i, j);
                    final Double gain_j = gains.get(i, j);
                    // gains = (gains + 0.2) * ((dY > 0) != (iY > 0)) + (gains * 0.8) * ((dY > 0) == (iY > 0))
                    if ((dY_j > 0) == (iY_j > 0)) {
                        gains.set(i, j, gain_j * 0.8);
                    }
                    else {
                        gains.set(i, j, gain_j + 0.2);
                    }
                }
            }

            // gains[gains < min_gain] = min_gain
            gains = gains.applyOnElements(e -> Math.max(e, MIN_GAIN));

            // iY = momentum * iY - eta * (gains * dY)
            iY = iY.scale(momentum).sub(dY.applyOnElements(gains, (e1, e2) -> e1 * e2).scale(LEARNING_RATE));

            // Y = Y + iY
            Y = Y.add(iY);

            // ---------------------------------------------------
            // (2) GLOBAL OPERATION!!!
            // ---------------------------------------------------

            //missing: mean per row or column

            // Y = Y - Math.tile(Math.mean(Y, 0), (n, 1))
            mean = mean.assign(0.0);
            for (int i=0; i < Y.numRows(); ++i) {
                mean.add(Y.rowAsVector(i));
            }
            mean = mean.div(Y.numRows());

            Y = Y.addVectorToRows(mean.mul(-1.0));

            // Compute current value of cost function
            if ((iteration + 1) % 10 == 0) {
                //C = Math.sum(P * Math.log(P / Q));
                double C = 0.0;
                for (int i = 0; i < P.numRows(); ++i) {
                    for (int j = 0; j < P.numCols(); ++j) {
                        if (i != j) {
                            C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                        }
                    }
                }
                LOG.info("Iteration " + (iteration + 1) + ", Error: " + C);
            }

            // Scale back to original values
            if (iteration == 100) {
                P = P.scale(1.0 / EARLY_EXAGGERATION);
            }
            LOG.debug("Y: " + Y.rowAsVector().toString());
            dataManager.globalSync();
        }
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
        Xsquared = Xsquared.applyOnElements(e -> Math.pow(e, 2), X);
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

        // execute over all elements i != j
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
                .run(TSNEJob_MC.class)
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