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

import java.io.Serializable;
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
    private final double ETA = 300.0;
    private final double EARLY_EXAGGERATION = 4.0;
    private final int MAX_ITER = 1;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        //todo: Implement computation of affinites, distances etc

        dataManager.loadAsMatrix("/Users/Chris/Downloads/mnist_20_jointP.csv", 20, 20,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));
        //dataManager.registerPullRequestHandler("sumQ",
        //        name -> Q.aggregateRows(f -> f.sum()).sum());

        dataManager.loadAsMatrix("/Users/Chris/Downloads/mnist_20_initY.csv", 20, 2,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        /*Y = new MatrixBuilder()
                .dimension(20, 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Random rand = new Random();
        // initialize Y by sampling from N(0, 10e-4)
        Y.applyOnElements(e -> e = rand.nextDouble() * 1e-2);
        LOG.info("Y:");
        LOG.info(Y.toString());
        dataManager.putObject("Y", Y);*/
    }


    @Override
    public void compute() {
        P = dataManager.getObject("/Users/Chris/Downloads/mnist_20_jointP.csv");
        Y = dataManager.getObject("/Users/Chris/Downloads/mnist_20_initY.csv");

        //final AtomicDouble sumQ = new AtomicDouble(0.0);

        // early exaggeration
        //P.scale(4.0);

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

            LOG.info("num:" + num.rowAsVector().toString());

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

            LOG.info("Q: " + Q.rowAsVector().toString());
            LOG.info("num2:" + num.rowAsVector().toString());

            // PQ = P - Q
            Matrix PQ = P.sub(Q);

            // dY[i,:] = Math.sum(Math.tile(PQ[:,i] * num[:,i], (no_dims, 1)).T * (Y[i,:] - Y), 0)
            for (int i=0; i < P.numRows(); ++i) {
                Vector sumVec = new VectorBuilder()
                        .dimension(d)
                        .format(Vector.Format.DENSE_VECTOR)
                        .layout(Vector.Layout.ROW_LAYOUT)
                        .build();
                sumVec.assign(0.0);

                for (int j=0; j < P.numCols(); ++j) {
                    final Double pq = PQ.get(i,j);
                    final Double num_j = num.get(i,j);
                    LOG.info("pq(" + i + "," + j + "): " + pq);
                    LOG.info("num_j(" + i + "," + j + "): " + num_j);
                    // (p - num / sum(q)) * num
                    // (p - q) * num
                    sumVec = sumVec.add(Y.rowAsVector(i).sub(Y.rowAsVector(j))
                                .applyOnElements(e -> e * pq * num_j));
                }
                dY.assignRow(i, sumVec);
                LOG.info("sumVec(" + i + "): " + sumVec.toString());
            }

            LOG.info("dY: " + dY.rowAsVector().toString());

            double momentum;

            if (iteration < 20) {
                momentum = INITIAL_MOMENTUM;
            } else {
                momentum = FINAL_MOMENTUM;
            }
/*
            //missing element wise matrix multiplication
            //for (int i = 0; i < n; ++i) {
            //    for (int j = 0; j < d; ++j) {
            //        dY.set(i, j, dY.get(i,j) * gains.get(i,j));
            //    }
            //}
            Matrix dY_2 = dY.applyOnElements(gains, (e1, e2) -> e1 * e2);


            iY = iY.scale(momentum).sub(dY_2.scale(eta));
            Y = Y.add(iY);

            //in Python
            //Y = Y - Math.tile(Math.mean(Y, 0), (n, 1));

            //missing fill vector with value
            //for (int j = 0; j < d; ++j) {
            //    meanVector.set(j,0.0);
            //}
            meanVector.assign(0.0);


            // ---------------------------------------------------
            // (2) GLOBAL OPERATION!!!
            // ---------------------------------------------------

            //missing: mean per row or column
            //global function here !!
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < d; ++j) {
                    meanVector.set(j, meanVector.get(j) + Y.get(i, j));
                }
            }

            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < d; ++j) {
                    Y.set(i, j, Y.get(i, j) - (meanVector.get(j) / n));
                }
            }

            // ---------------------------------------------------

            // Compute current value of cost function
            if ((iter + 1) % 10 == 0) {
                //C = Math.sum(P * Math.log(P / Q));
                double C = 0.0;
                for (int i = 0; i < n; ++i) {
                    for (int j = 0; j < d; ++j) {
                        C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                    }
                }
                System.out.println("Iteration " + (iter + 1) + ": error is " + C);

                // Stop lying about P-values
                if (iter == 100) {
                    P = P.scale(1.0 / 4.0);
                }
            }
*/
            dataManager.sync();
        }

    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(TSNEJob.class)
                .results(res)
                .done();

        //missing result to CSV

        /*final DecimalFormat numberFormat = new DecimalFormat("0.000");
        res.forEach(
                r -> r.forEach(
                        w -> {
                            for (double weight : ((Vector)w).toArray())
                                System.out.print(numberFormat.format(weight) + "\t | ");
                            System.out.println();
                        }
                )
        );*/
    }
}