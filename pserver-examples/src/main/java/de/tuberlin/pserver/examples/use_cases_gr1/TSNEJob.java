package de.tuberlin.pserver.examples.use_cases_gr1;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.examples.ml.GenerateLocalTestData;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.VectorBuilder;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.SGD.SGDOptimizer;

import java.io.Serializable;
import java.text.DecimalFormat;
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

        dataManager.loadAsMatrix("datasets/mnist_1000.csv", 1000, 1000, RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        Y = new MatrixBuilder()
                .dimension(1000, 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Random rand = new Random();

        for (int i = 0; i < Y.numCols(); ++i) {
            for (int j = 0; j < Y.numRows(); ++j) {
                Y.set(i, j, rand.nextDouble() * 10e-2);
            }
        }

        dataManager.putObject("Y", Y);
    }

    @Override
    public void compute() {
        Y = dataManager.getObject("Y");
        Matrix P = dataManager.getObject("datasets/mnist_1000.csv");

        for (int iter = 0; iter < 100; ++iter) {


            Matrix squared = new MatrixBuilder()
                    .dimension(1000, 2)
                    .format(Matrix.Format.DENSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .build();


            Vector sum_Y = new VectorBuilder()
                    .dimension(1000)
                    .format(Vector.Format.DENSE_VECTOR)
                    .layout(Vector.Layout.ROW_LAYOUT)
                    .build();

            //Math.square(Y)
            for (int i = 0; i < Y.numRows(); ++i) {
                for (int j = 0; j < Y.numCols(); ++j) {
                    squared.set(i, j, Math.pow(Y.get(i, j), 2));
                }
            }

            //missing numpy.sum(Y,1)
            for (int i = 0; i < squared.numRows(); ++i) {
                double sum = 0.0;
                for (int j = 0; j < squared.numCols(); ++j) {
                    sum += squared.get(i, j);
                }
                sum_Y.set(i,sum);
            }


            Matrix Y1 = Y.mul(Y.transpose()).scale(-2); //Math.dot(Y, Y.T) * -2

            //missing matrix.add(vector)

            for (int i = 0; i < squared.numRows(); ++i) {
                for (int j = 0; j < squared.numCols(); ++j) {
                    Y1.set(i, j, Y1.get(i, j) + sum_Y.get(j));
                }
            }

            Matrix Y2 = Y1.transpose();

            //missing matrix.add(vector)

            for (int i = 0; i < Y2.numRows(); ++i) {
                for (int j = 0; j < Y2.numCols(); ++j) {
                    Y2.set(i, j, Y2.get(i, j) + sum_Y.get(i));
                }
            }

            //missing matrix.add(scalar)

            for (int i = 0; i < Y2.numRows(); ++i) {
                for (int j = 0; j < Y2.numCols(); ++j) {
                    Y2.set(i, j, Y2.get(i, j) + 1);
                }
            }


            //missing 1 / matrix

            for (int i = 0; i < Y2.numRows(); ++i) {
                for (int j = 0; j < Y2.numCols(); ++j) {
                    Y2.set(i, j, 1.0 / Y2.get(i, j));
                }
            }

            Matrix num = Y2;

            for (int i = 0; i < num.numRows(); ++i) {
                for (int j = 0; j < num.numCols(); ++j) {
                    if (i == j) {
                        num.set(i,j,0.0);
                    }
                }
            }

            //missing: sum over all elements of a matrix
            //global operation here!!

            double sumOverNum = 0.0;
            for (int i = 0; i < num.numRows(); ++i) {
                for (int j = 0; j < num.numCols(); ++j) {
                    sumOverNum += num.get(i,j);
                }
            }

            Matrix Q = num.scale(1.0 / sumOverNum);

            //missing: element wise Maxnum
            for (int i = 0; i < Q.numRows(); ++i) {
                for (int j = 0; j < Q.numCols(); ++j) {
                    Q.set(i,j, Math.max(Q.get(i,j), 1e-12));
                }
            }

            Matrix PQ = P.sub(Q);

            //missing elementwise vector multiplication = vector * vector
            for (int i = 0; i < Y.numRows(); ++i) {

                Vector pqI = PQ.colAsVector(i);
                Vector numI = num.colAsVector(i);
            }






/*
            Matrix Y1 = Y.mul(Y.transpose()).scale(-2).add(sum_Y).transpose().add(sum_Y);
/*
            /*
            for (int i = 0; i < Y.numCols(); ++i) {
                for (int j = 0; j < Y.numRows(); ++j) {
                    Y1.set(i, j, 1 / (1 + Y1.get(i, j)));
                }
            }*/

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