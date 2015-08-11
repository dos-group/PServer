package de.tuberlin.pserver.examples.experiments.glove;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.JobExecutable;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.runtime.filesystem.record.RecordFormat;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;

public class GloVeJobAdaGradOriginalPull extends JobExecutable {

    /* input data parameter */
    private static final int NUM_WORDS_IN_COOC_MATRIX = 36073;
    private static final String INPUT_DATA = "/data/fridtjof.sander/text8_coocc.csv";
    private static final String OUTPUT_DATA = "/data/fridtjof.sander/pserver_glove.csv";

    /* hyperparameter */
    private static final int VEC_DIM = 50;
    private static final double ALPHA = 0.75;
    private static final int XMAX = 10;
    private static final double LearningRate = 0.05;
    private static final int MAX_ITER = 15;
    private static final double MATRIX_TRANSMIT_THRESHOLD = 0.1;

    // weight matrix (aka model)
    private Matrix W;
    private Matrix W_old;       // only used for delta-pull
    private Matrix W_deltas;    // only used for delta-push
    private Lock W_lock;        // only used for push & delta-push

    // biases for the weight matrix
    private Vector B;
    private Vector B_old;       // only used for delta-pull
    private Vector B_deltas;    // only used for delta-push
    private Lock B_lock;        // only used for push & delta-push

    // gradient adjustments for AdaGrad
    private Matrix GradSq;
    private Matrix GradSq_old;      // only used for delta-pull
    private Matrix GradSq_deltas;   // only used for delta-push
    private Lock GradSq_lock;       // only used for push & delta-push
    private Vector GradSqB;
    private Vector GradSqB_old;     // only used for delta-pull
    private Vector GradSqB_deltas;  // only used for delta-push
    private Lock GradSqB_lock;      // only used for push & delta-push

    // cooccurrence matrix - every node only gets a part of it (subset of rows)
    private Matrix X;


    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        System.out.println(NUM_WORDS_IN_COOC_MATRIX);

        /* load cooc matrix */
        dataManager.loadAsMatrix(INPUT_DATA, NUM_WORDS_IN_COOC_MATRIX, NUM_WORDS_IN_COOC_MATRIX,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD),
                Matrix.Format.SPARSE_MATRIX, Matrix.Layout.ROW_LAYOUT);

        /* create matrices */
        W = createMatrix(Matrix.Format.DENSE_MATRIX);
        W_old = createMatrix(Matrix.Format.DENSE_MATRIX);
        W_deltas = createMatrix(Matrix.Format.SPARSE_MATRIX);

        GradSq = createMatrix(Matrix.Format.DENSE_MATRIX);
        GradSq_old = createMatrix(Matrix.Format.DENSE_MATRIX);
        GradSq_deltas = createMatrix(Matrix.Format.SPARSE_MATRIX);

        /* create bias vectors */
        GradSqB = createVector(Vector.Format.DENSE_VECTOR);
        GradSqB_old = createVector(Vector.Format.DENSE_VECTOR);
        GradSqB_deltas = createVector(Vector.Format.SPARSE_VECTOR);

        B = createVector(Vector.Format.DENSE_VECTOR);
        B_old = createVector(Vector.Format.DENSE_VECTOR);
        B_deltas = createVector(Vector.Format.SPARSE_VECTOR);

        /* initialize matrices & bias vectors */
        final Random rand = new Random();
        for (int i = 0; i < W.numRows(); ++i) {
            for (int j = 0; j < W.numCols(); ++j) {
                W.set(i, j, (rand.nextDouble() - 0.5) / VEC_DIM);
                GradSq.set(i, j, 1.0);
            }
        }
        for (int i = 0; i < GradSqB.length(); i++) {
            B.set(i, (rand.nextDouble() - 0.5) / VEC_DIM);
            GradSqB.set(i, 1.0);
        }

        /* make matrices & bias vectors available for all nodes */
        dataManager.putObject("W", W);
        dataManager.putObject("B", B);
        dataManager.putObject("GradSq", GradSq);
        dataManager.putObject("GradSqB", GradSqB);
    }

    @Override
    public void compute() {

        /* get the nodes part of the cooc matrix */
        X = dataManager.getObject(INPUT_DATA);

        /* get matrices & bias vectors */
        W = dataManager.getObject("W");
        B = dataManager.getObject("B");
        GradSq = dataManager.getObject("GradSq");
        GradSqB = dataManager.getObject("GradSqB");

        int numInstances = slotContext.jobContext.numOfNodes;
        int offset = NUM_WORDS_IN_COOC_MATRIX / numInstances * slotContext.jobContext.nodeID;

        int iterations = 0;

        while (iterations++ < MAX_ITER /* TODO: abort on convergence criteria ? */) {

            LOG.info("Starting iteration " + (iterations - 1));

            double costI = 0;

            for (int ridxLocal = 0; ridxLocal < X.numRows(); ridxLocal++) {

                for (int cidx = 0; cidx < NUM_WORDS_IN_COOC_MATRIX; cidx++) {
                    
                    long wordVecIdx = offset + ridxLocal;
                    long ctxVecIdx = cidx + NUM_WORDS_IN_COOC_MATRIX;

                    Double xVal = X.get(ridxLocal, cidx);

                    if (xVal == 0)
                        continue;    // skip 0 values in the cooccurrence matrix cause Math.log(0) = -Infinity

                    // word vector
                    Vector w1 = W.colAsVector(wordVecIdx);
                    Double b1 = B.get(wordVecIdx);
                    Vector gs1 = GradSq.colAsVector(wordVecIdx);

                    // context vector
                    Vector w2 = W.colAsVector(ctxVecIdx);
                    Double b2 = B.get(ctxVecIdx);
                    Vector gs2 = GradSq.colAsVector(ctxVecIdx);


                    /* calculate gradient */
                    double diff = w1.dot(w2) + b1 + b2 - Math.log(xVal);
                    double fdiff = (xVal > XMAX) ? diff : Math.pow(xVal / XMAX, ALPHA) * diff;

                    // GLOBAL OPERATION - EXCHANGE cost
                    costI += 0.5 * diff * fdiff;

                    fdiff *= LearningRate;

                    Vector grad1 = w2.mul(fdiff);
                    Vector grad2 = w1.mul(fdiff);

                    W.assignColumn(wordVecIdx, w1.add(-1, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(ctxVecIdx, w2.add(-1, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(wordVecIdx, b1 - fdiff / Math.sqrt(GradSqB.get(wordVecIdx)));
                    B.set(ctxVecIdx, b2 - fdiff / Math.sqrt(GradSqB.get(ctxVecIdx)));

                    /* update gradient adjustments for AdaGrad */
                    gs1 = gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2);
                    gs2 = gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2);

                    GradSq.assignColumn(wordVecIdx, gs1);
                    GradSq.assignColumn(ctxVecIdx, gs2);

                    GradSqB.set(wordVecIdx, GradSqB.get(wordVecIdx) + fdiff * fdiff);
                    GradSqB.set(ctxVecIdx, GradSqB.get(ctxVecIdx) + fdiff * fdiff);
                    
                }

            }

            costI /= X.numRows() * NUM_WORDS_IN_COOC_MATRIX;
            LOG.info("Iteration, Cost: " + (iterations - 1) + ", " + costI);

            /* pull data from all other nodes after each iteration */
            dataManager.pullMerge(W, matrixMerger);
            dataManager.pullMerge(GradSq, matrixMerger);
            dataManager.pullMerge(B, vectorMerger);
            dataManager.pullMerge(GradSqB, vectorMerger);
        }

        result(W);
    }

    // ---------------------------------------------------
    // Pull Stuff.
    // ---------------------------------------------------

    private static final DataManager.Merger<Vector> vectorMerger = (dst, src) -> {
        for (final Vector b : src) {
            dst.applyOnElements(b, (e1, e2) -> e1 + e2, dst);
        }

        dst.applyOnElements(e -> e / (src.size() + 1), dst);
    };

    private static final DataManager.Merger<Matrix> matrixMerger = (dst, src) -> {
        for (final Matrix m : src) {
            dst.applyOnElements(m, (e1, e2) -> e1 + e2);
        }

        dst.applyOnElements(e -> e / (src.size() + 1));
    };

    // Prints the matrix as a csv file. Each line is formatted as "<index>;<col[0]>;<col[1]>;<col[2]>;...". Adds context and word vector.
    private static void printMatrixPerVector(Matrix w_avg, String fileName) {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(fileName, "UTF-8");
            for (int col = 0; col < NUM_WORDS_IN_COOC_MATRIX; ++col) {
                String vec = "";
                for (int row = 0; row < w_avg.numRows(); ++row) {
                    vec = vec + ";" + (w_avg.get(row, col) + w_avg.get(row, col * 2));
                }
                writer.println(col + vec);
            }
        } catch (Exception ignored) {
        } finally {
            if(writer != null) writer.close();
        }
    }

    private static Matrix mergeMatrices(List<List<Serializable>> res) {
        Matrix W_avg = createMatrix(Matrix.Format.DENSE_MATRIX);
        int numMergedMatrices = 0;
        for (int i = 1; i < res.size(); i++) {
            List<Serializable> r = res.get(i);
            for (int j = 0; j < r.size(); j++) {
                Matrix m = (Matrix) r.get(j);
                W_avg.applyOnElements(m, (e1, e2) -> e1 + e2);
                numMergedMatrices++;
            }
        }
        final int finalNumMergedMatrices = numMergedMatrices;
        W_avg.applyOnElements(e -> e / finalNumMergedMatrices);
        return W_avg;
    }

    // ---------------------------------------------------
    // Other Helper Methods.
    // ---------------------------------------------------

    private static Matrix createMatrix(Matrix.Format format) {
        return new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(format)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
    }

    private static Vector createVector(Vector.Format format) {
        return new VectorBuilder()
                .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(format)
                .layout(Vector.Layout.ROW_LAYOUT)
                .build();
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.DISTRIBUTED
                .run(GloVeJobAdaGradOriginalPull.class)
                .results(res)
                .done();

        printMatrixPerVector(mergeMatrices(res), OUTPUT_DATA);
    }

}