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

    private static final String INPUT_DATA = "/data/home/fgoessler/cooc-agg.csv";

    private static final double ALPHA = 0.75;

    private static final int XMAX = 10;

    private double LearningRate = 0.05;

    private static final int MAX_ITER = 15;

    private Matrix W;

    private Matrix GradSq;

    private Vector GradSqB;

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
                .dimension(VEC_DIM, NUM_ROWS * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();

        GradSqB = new VectorBuilder()
                .dimension(NUM_ROWS*2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();

        B = new VectorBuilder()
                .dimension(NUM_ROWS*2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();

        for (int i = 0; i < W.numRows(); ++i) {
            for (int j = 0; j < W.numCols(); ++j) {
                W.set(i, j, (rand.nextDouble() - 0.5) / VEC_DIM);
                GradSq.set(i, j, 1.0);
            }
        }
        for(int i = 0; i < GradSqB.length(); i++) {
            B.set(i, (rand.nextDouble()-0.5)/VEC_DIM);
            GradSqB.set(i, 1.0);
        }

        dataManager.putObject("W", W);
        dataManager.putObject("B", B);
        dataManager.putObject("GradSq", GradSq);
        dataManager.putObject("GradSqB", GradSqB);
    }

    @Override
    public void compute() {

        X = dataManager.getObject(INPUT_DATA);

        W = dataManager.getObject("W");

        B = dataManager.getObject("B");

        GradSq = dataManager.getObject("GradSq");

        GradSqB = dataManager.getObject("GradSqB");

        int offset = 0;// NUM_ROWS / num_instaces * ctx.instanceID;

        int iterations = 0;

        while(iterations++ < MAX_ITER) {

            LOG.info("Starting iteration " + (iterations-1));

            double costI = 0;

            for (int ridxLocal = 0; ridxLocal < X.numRows(); ridxLocal++) {
                for (int cidx = 0; cidx < NUM_COLS; cidx++) {

                    int ridx = offset + ridxLocal;

                    Double XVal = X.get(ridxLocal, cidx);

                    if (XVal == 0) continue;

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

                    if (ridxLocal == 0 && cidx < 4 && false) {
                        LOG.info("Diff: "+diff);
                        LOG.info("Fdiff: "+fdiff);
                        LOG.info("Cost: "+costI);
                        LOG.info("w1: "+w1.toString());
                        LOG.info("w2: "+w2.toString());
                        LOG.info("X: "+XVal);
                    }

                    Vector grad1 = w2.mul(fdiff);
                    Vector grad2 = w1.mul(fdiff);

                    Vector gs1 = GradSq.colAsVector(ridx);
                    Vector gs2 = GradSq.colAsVector(cidx + NUM_ROWS);

                    W.assignColumn(ridx, w1.add(-1, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(cidx + NUM_ROWS, w2.add(-1, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(ridx, b1 - fdiff / Math.sqrt(GradSqB.get(ridx)));
                    B.set(cidx + NUM_ROWS, b2 - fdiff / Math.sqrt(GradSqB.get(cidx + NUM_ROWS)));

                    gs1 = gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2);
                    gs2 = gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2);

                    GradSq.assignColumn(ridx, gs1);
                    GradSq.assignColumn(cidx + NUM_ROWS, gs2);

                    GradSqB.set(ridx, GradSqB.get(ridx) + fdiff * fdiff);
                    GradSqB.set(cidx + NUM_ROWS, GradSqB.get(cidx + NUM_ROWS) + fdiff * fdiff);

                    if (ridxLocal == 0 && cidx < 4 && false) {
                        LOG.info("w1 nach upd: "+W.colAsVector(ridx));
                        LOG.info("w2 nach upd: "+W.colAsVector(cidx+NUM_ROWS));
                        LOG.info("b1 nach upd: "+B.get(ridx));
                        LOG.info("b2 nach upd: "+B.get(cidx+NUM_ROWS));
                        LOG.info("gradsq nach upd: "+GradSq.colAsVector(ridx));
                        LOG.info("gradsq w2 nach upd: "+GradSq.colAsVector(cidx+NUM_ROWS));
                        LOG.info("gradsqB nach upd: "+GradSqB.get(ridx));
                        LOG.info("gradsq b2 nach upd: "+GradSqB.get(cidx+NUM_ROWS));
                    }

                }
            }

            costI /= X.numRows()*NUM_COLS;
            LOG.info("Iteration, Cost: " + (iterations - 1) + ", " + costI);

            dataManager.pullMerge(W, (dst, src) -> {

                for (final Matrix m : src) {
                    dst.applyOnElements(m, (e1, e2) -> e1 + e2);
                }

                dst.applyOnElements(e -> e / (src.size() + 1));
            });

            dataManager.pullMerge(B, (dst, src) -> {
                for (final Vector b : src) {
                    dst.applyOnElements(b, (e1, e2) -> e1 + e2);
                }

                dst.applyOnElements(e -> e / (src.size() + 1));
            });

            dataManager.pullMerge(GradSq, (dst, src) -> {

                for (final Matrix m : src) {
                    dst.applyOnElements(m, (e1, e2) -> e1 + e2);
                }

                dst.applyOnElements(e -> e / (src.size() + 1));
            });

            dataManager.pullMerge(GradSqB, (dst, src) -> {
                for (final Vector b : src) {
                    dst.applyOnElements(b, (e1, e2) -> e1 + e2);
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