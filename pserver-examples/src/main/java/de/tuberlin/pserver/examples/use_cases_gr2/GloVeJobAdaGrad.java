package de.tuberlin.pserver.examples.use_cases_gr2;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
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

public class GloVeJobAdaGrad extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final int NUM_COLS = 1000;

    private static final int NUM_ROWS = 1000;

    private static final int VEC_DIM = 50;

    private static final String INPUT_DATA = "XXX.csv";

    private static final double ALPHA = 0.75;

    private static final int XMAX = 10;

    private double LearningRate = 0.1;

    private static final int MAX_ITER = 15;

    private Matrix W;

    private Matrix GradSq;

    private Matrix X;

    private Vector B;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadAsMatrix(INPUT_DATA, NUM_ROWS, NUM_COLS,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        W = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_ROWS * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        final Random rand = new Random();

        GradSq = new MatrixBuilder()
                .dimension(VEC_DIM+1, NUM_ROWS * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        B = new VectorBuilder()
                .dimension(NUM_ROWS*2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT) // ?
                .build();

        for (int i = 0; i < W.numRows(); ++i) {
            for (int j = 0; j < W.numCols(); ++j) {
                W.set(i, j, (rand.nextDouble()-0.5)/VEC_DIM);
                GradSq.set(i, j, 1.0);
            }
            //B.set(i, j, (rand.nextDouble()-0.5)/VEC_DIM);
        }

        dataManager.putObject("W", W);
        dataManager.putObject("GradSq", GradSq);

        dataManager.putObject("B", B);
    }

    @Override
    public void compute() {

        X = dataManager.getObject(INPUT_DATA);

        W = dataManager.getObject("W");

        B = dataManager.getObject("B");

        GradSq = dataManager.getObject("GradSq");

        int offset = NUM_ROWS/ctx.instanceID;

        int iterations = 0;

        while(iterations++ < MAX_ITER) {

            double costI = 0;

            for (int ridx = offset; ridx < offset + NUM_ROWS / X.numRows(); ridx++) {
                for (int cidx = 0; cidx < NUM_COLS; cidx++) {
                    Double XVal = X.get(ridx, cidx);

                    // word vector
                    Vector w1 = W.colAsVector(ridx);
                    Double b1 = B.get(ridx);
                    // context vector
                    Vector w2 = W.colAsVector(cidx + NUM_ROWS);
                    Double b2 = B.get(cidx + NUM_ROWS);

                    double diff = w1.dot(w2) + b1 + b2 - Math.log(XVal);
                    double fdiff = (XVal > XMAX) ? diff : Math.pow(XVal / XMAX, ALPHA) * diff;

                    // GLOBAL OPERATION - EXCHANGE cost
                    costI += 0.5 * diff * fdiff;

                    fdiff *= LearningRate;

                    Vector grad1 = w2.mul(fdiff);
                    Vector grad2 = w1.mul(fdiff);

                    //W.assignColumn(ridx, w1.add(-1, grad1.applyOnElements(GradSq.colAsVector(ridx, 0, VEC_DIM), (el1, el2) -> el1/Math.sqrt(el2))));
                    //W.assignColumn(cidx + NUM_ROWS, w2.add(-1, grad2.applyOnElements(GradSq.colAsVector(cidx + NUM_ROWS, 0, VEC_DIM), (el1, el2) -> el1/Math.sqrt(el2))));

                    B.set(ridx, b1 - fdiff/Math.sqrt(GradSq.get(ridx,VEC_DIM + 1)));
                    B.set(cidx + NUM_ROWS, b2 - fdiff/Math.sqrt(GradSq.get(cidx+NUM_ROWS,VEC_DIM + 1)));

                    //GradSq.assignColumn(ridx, )
                }
            }

            costI /= X.numRows()*NUM_COLS;

            dataManager.pullMerge(W, (dst, src) -> {

                for (final Matrix m : src) {
                    dst.applyOnElements(m, (e1, e2) -> e1 + e2);
                }

                dst.applyOnElements(e -> e / (src.size() + 1));
            });
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(GloVeJobAdaGrad.class)
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