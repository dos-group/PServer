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

import java.io.Serializable;
import java.util.List;
import java.util.Random;


public class TSNEJob extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Matrix Y;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        //todo: Implement computation of affinites, distances etc

        dataManager.loadAsMatrix("datasets/mnist_1000.csv", 1000, 1000, RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        Y = new MatrixBuilder()
                .dimension(1000, 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Random rand = new Random();

//        for (int i = 0; i < Y.numRows(); ++i) {
//            for (int j = 0; j < Y.numCols(); ++j) {
//                Y.set(i, j, rand.nextDouble() * 10e-2);
//            }
//        }

        Y.applyOnElements(element -> element = rand.nextDouble());

        dataManager.putObject("Y", Y);
    }


    //for loop count: 40 ;-)
    @Override
    public void compute() {
        Y = dataManager.getObject("Y");
        Matrix P = dataManager.getObject("datasets/mnist_1000.csv");

        P.scale(4.0);

        Long n = Y.numRows();
        Long d = Y.numCols();

        final double minGain = 0.01;
        final double initial_momentum = 0.5;
        final double final_momentum = 0.8;
        final double eta = 500;

        Matrix squared = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        //Vector sum_Y = new VectorBuilder()
        //        .dimension(n)
        //        .format(Vector.Format.DENSE_VECTOR)
        //        .layout(Vector.Layout.ROW_LAYOUT)
        //        .build();

        Matrix gains = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        //gradient of the last iteration (enriched by fancy regularization)
        Matrix iY = new MatrixBuilder()
                .dimension(n, d)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        Vector meanVector = new VectorBuilder()
                .dimension(d)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.ROW_LAYOUT)
                .build();

        //missing: fill matrix with value
        //for (int i = 0; i < n; ++i) {
        //    for (int j = 0; j < d; ++j) {
        //        gains.set(i, j, 1.0);
        //    }
        //}
        gains.assign(1.0);

        //missing: fill matrix with value
        //for (int i = 0; i < n; ++i) {
        //    for (int j = 0; j < d; ++j) {
        //        iY.set(i,j,0.0);
        //    }
        //}
        iY.assign(0.0);

        for (int iter = 0; iter < 100; ++iter) {

            //Math.square(Y)
            //for (int i = 0; i < Y.numRows(); ++i) {
            //    for (int j = 0; j < Y.numCols(); ++j) {
            //        squared.set(i, j, Math.pow(Y.get(i, j), 2));
            //    }
            //}
            squared.applyOnElements(Y, e -> Math.pow(e, 2));

            //missing numpy.sum(Y,1)
            //for (int i = 0; i < squared.numRows(); ++i) {
            //    double sum = 0.0;
            //    for (int j = 0; j < squared.numCols(); ++j) {
            //        sum += squared.get(i, j);
            //    }
            //    sum_Y.set(i,sum);
            //}
            Vector sum_Y = squared.aggregateRows(f -> f.sum());

            Matrix Y1 = Y.mul(Y.transpose()).scale(-2); //Math.dot(Y, Y.T) * -2

            //missing matrix.add(vector)
            //for (int i = 0; i < squared.numRows(); ++i) {
            //    for (int j = 0; j < squared.numCols(); ++j) {
            //        Y1.set(i, j, Y1.get(i, j) + sum_Y.get(j));
            //    }
            //}
            Matrix Y1_2 = Y1.addVectorToRows(sum_Y);

            Matrix Y2 = Y1_2.transpose();

            //missing matrix.add(vector)
            //for (int i = 0; i < Y2.numRows(); ++i) {
            //    for (int j = 0; j < Y2.numCols(); ++j) {
            //        Y2.set(i, j, Y2.get(i, j) + sum_Y.get(i));
            //    }
            //}
            Matrix Y2_2 = Y2.addVectorToRows(sum_Y);

            //missing matrix.add(scalar)
            //for (int i = 0; i < Y2.numRows(); ++i) {
            //    for (int j = 0; j < Y2.numCols(); ++j) {
            //        Y2.set(i, j, Y2.get(i, j) + 1);
            //    }
            //}
            //Matrix Y3 = Y2.applyOnElements(e -> e + 1);

            //missing 1 / matrix
            //for (int i = 0; i < Y2.numRows(); ++i) {
            //    for (int j = 0; j < Y2.numCols(); ++j) {
            //        Y2.set(i, j, 1.0 / Y2.get(i, j));
            //    }
            //}
            //Matrix Y3 = Y2.applyOnElements(e -> 1.0 / e);

            Matrix Y3 = Y2.applyOnElements(e -> (e + 1.0) / e);

            //Matrix num = Y3;
            //for (int i = 0; i < num.numRows(); ++i) {
            //    for (int j = 0; j < num.numCols(); ++j) {
            //        if (i == j) {
            //            num.set(i,j,0.0);
            //        }
            //    }
            //}

            Matrix num = Y3.zeroDiagonal();

            // ---------------------------------------------------
            // (1) GLOBAL OPERATION!!!
            // ---------------------------------------------------
            //missing: sum over all elements of a matrix

            double sumOverNum = 0.0;
            for (int i = 0; i < num.numRows(); ++i) {
                for (int j = 0; j < num.numCols(); ++j) {
                    sumOverNum += num.get(i, j);
                }
            }


            // ---------------------------------------------------

            Matrix Q = num.scale(1.0 / sumOverNum);

            //missing: element wise Maxnum
            //for (int i = 0; i < Q.numRows(); ++i) {
            //    for (int j = 0; j < Q.numCols(); ++j) {
            //        Q.set(i,j, Math.max(Q.get(i,j), 1e-12));
            //    }
            //}
            Matrix Q2 = Q.applyOnElements(e -> Math.max(e, 1e-12));

            Matrix PQ = P.sub(Q2);

            Matrix gradient = new MatrixBuilder()
                    .dimension(1000, 2)
                    .format(Matrix.Format.DENSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .build();

            //missing element wise vector multiplication = vector * vector
            for (int i = 0; i < n; ++i) {

                Vector sumVec = new VectorBuilder()
                        .dimension(d)
                        .format(Vector.Format.DENSE_VECTOR)
                        .layout(Vector.Layout.ROW_LAYOUT)
                        .build();

                //missing fill vector or matrix with zeros, ones, ...
                //for (int u = 0; u < d; u++) {
                //    sumVec.set(u, 0.0);
                //}
                sumVec.assign(0.0);

                for (int j = 0; j < n; j++) {
                    sumVec = sumVec.add(Y.rowAsVector(i).mul(PQ.get(i, j) * num.get(i, j)));
                }

                gradient.assignRow(i, sumVec);
            }

            Matrix dY = gradient;

            //calculate difference between last gradient
            // in Python:
            // 		gains = (gains + 0.2) * ((dY > 0) != (iY > 0)) + (gains * 0.8) * ((dY > 0) == (iY > 0));
            //			gains[gains < min_gain] = min_gain;
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < d; ++j) {
                    if ((dY.get(i, j) > 0) != (iY.get(i, j) > 0)) {
                        gains.set(i, j, Math.max(gains.get(i, j) + 0.2, minGain));
                    } else {
                        gains.set(i, j, Math.max(gains.get(i, j) * 0.8, minGain));
                    }
                }
            }

            double momentum;

            if (iter < 20) {
                momentum = initial_momentum;
            } else {
                momentum = final_momentum;
            }

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