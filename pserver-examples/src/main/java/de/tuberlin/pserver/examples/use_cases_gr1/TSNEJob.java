package de.tuberlin.pserver.examples.use_cases_gr1;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.VectorBuilder;
import sun.rmi.runtime.Log;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;


public class TSNEJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Matrix P;
    private Matrix Q;
    private Matrix Y;

    private final double MIN_GAIN = 0.01;
    private final double INITIAL_MOMENTUM = 0.5;
    private final double FINAL_MOMENTUM = 0.8;
    private final double ETA = 350.0;
    private final double EARLY_EXAGGERATION = 4.0;
    private final int MAX_ITER = 300;

    private final String INPUT_MATRIX = "/Users/Chris/Downloads/mnist_500_jointP.csv";
    private final Integer INPUT_DIMS = 500;
    private final Integer EMBEDDING_DIMS = 2;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        //todo: Implement computation of affinites, distances etc

        dataManager.loadAsMatrix(INPUT_MATRIX, INPUT_DIMS, INPUT_DIMS,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));
        //dataManager.registerPullRequestHandler("sumQ",
        //        name -> Q.aggregateRows(f -> f.sum()).sum());

        //dataManager.loadAsMatrix("/Users/Chris/Downloads/mnist_20_initY.csv", 20, 2,
        //        RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        Y = new MatrixBuilder()
                .dimension(INPUT_DIMS, EMBEDDING_DIMS)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Random rand = new Random();
        // initialize Y by sampling from N(0, 10e-4)
        Y.applyOnElements(e -> e = rand.nextDouble());

        dataManager.putObject("Y", Y);
    }


    @Override
    public void compute() {
        P = dataManager.getObject(INPUT_MATRIX);
        //Y = dataManager.getObject("/Users/Chris/Downloads/mnist_20_initY.csv");
        Y = dataManager.getObject("Y");

        //final AtomicDouble sumQ = new AtomicDouble(0.0);

        // early exaggeration
        P = P.scale(EARLY_EXAGGERATION);

        Long n = Y.numRows();
        Long d = Y.numCols();

        Q = new MatrixBuilder()
                .dimension(n, n)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Matrix PQ = new MatrixBuilder()
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

        //
        for (int iteration = 0; iteration < MAX_ITER; ++iteration) {
            // sum_Y = Math.sum(Math.square(Y), 1)
            Y_squared = Y_squared.applyOnElements(Y, e -> Math.pow(e, 2));
            Vector sum_Y = Y_squared.aggregateRows(f -> f.sum());

            // num = 1 / (1 + Math.add(Math.add(-2 * Math.dot(Y, Y.T), sum_Y).T, sum_Y))
            Matrix num = Y.mul(Y.transpose()).scale(-2).addVectorToRows(sum_Y).transpose()
                    .addVectorToRows(sum_Y);
            num.applyOnElements(e -> 1.0 / (e + 1.0));
            // entries i=j must be zero
            num = num.zeroDiagonal();

            LOG.debug("num:" + num.rowAsVector().toString());

            // ---------------------------------------------------
            // (1) GLOBAL OPERATION!!!
            // ---------------------------------------------------
            //missing: sum over all elements of a matrix

            /*dataManager.sync();

            if (ctx.instanceID == 0) {
                Object[] sumQs = dataManager.pullRequest("sumQ");
                Double sumOverQ = 0.0;
                for (Object sum : sumQs) {
                    sumOverQ += (Double) sum;
                }
                sumOverQ += Q.aggregateRows(f -> f.sum()).sum();
                sumQ.set(sumOverQ);
                dataManager.pushToAll("sumOverQ", sumOverQ);
            }
            else {
                dataManager.awaitEvent(1, "sumOverQ", new DataManager.DataEventHandler(){
                    @Override
                    public void handleDataEvent(int srcInstanceID, Object value) {
                        sumQ.set((Double) value);
                    }
                });
            }

            // ---------------------------------------------------

            Q = Q.scale(1 / sumQ.get());
            */

            Double sumNum = num.aggregateRows(f -> f.sum()).sum();
            // Q = num / Math.sum(num)
            Q = num.copy().scale(1.0 / sumNum);

            // Math.maximum(Q, 1e-12)
            Q = Q.applyOnElements(e -> Math.max(e, 1e-12));

            LOG.debug("Q: " + Q.rowAsVector().toString());
            LOG.debug("num2:" + num.rowAsVector().toString());
            LOG.debug("P:" + P.rowAsVector().toString());

            // PQ = P - Q
            PQ = P.copy().sub(Q);

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

            LOG.debug("dY: " + dY.rowAsVector().toString());

            double momentum;

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
            iY = iY.scale(momentum).sub(dY.applyOnElements(gains, (e1, e2) -> e1 * e2).scale(ETA));

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
                        C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                    }
                }
                LOG.info("Iteration " + (iteration + 1) + ", Error: " + C);
            }

            // Scale back to original values
            if (iteration == 100) {
                P = P.scale(1.0 / EARLY_EXAGGERATION);
            }

            dataManager.sync();
        }
        LOG.debug("Y: " + Y.rowAsVector().toString());
        result(Y);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();
        PrintWriter writer = null;

        PServerExecutor.LOCAL
                .run(TSNEJob.class)
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