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
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class GloVeJobAdaGrad extends PServerJob {

    private static final DataManager.Merger<Vector> vectorMerger = (dst, src) -> {
        for (final Vector b : src) {
            dst.applyOnElements(b, (e1, e2) -> e1 + e2);
        }

        dst.applyOnElements(e -> e / (src.size() + 1));
    };

    private static final DataManager.Merger<Matrix> matrixMerger = (dst, src) -> {
        for (final Matrix m : src) {
            dst.applyOnElements(m, (e1, e2) -> e1 + e2);
        }

        dst.applyOnElements(e -> e / (src.size() + 1));
    };

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final boolean USE_PULL_REQUEST_FEATURE = true;

    /* input data parameter */
    private static final int NUM_WORDS_IN_COOC_MATRIX = 1000;
    private static final String INPUT_DATA = "/data/home/fgoessler/cooc-agg.csv";

    /* hyperparameter */
    private static final int VEC_DIM = 50;
    private static final double ALPHA = 0.75;
    private static final int XMAX = 10;
    private static final double LearningRate = 0.05;
    private static final int MAX_ITER = 15;
    private static final double MATRIX_TRANSMIT_THRESHOLD = 0.5;

    // weight matrix (aka model)
    private Matrix W;
    private Matrix W_old;

    // biases for the weight matrix
    private Vector B;
    private Vector B_old;

    // gradient adjustments for AdaGrad
    private Matrix GradSq;
    private Matrix GradSq_old;
    private Vector GradSqB;
    private Vector GradSqB_old;

    // cooccurrence matrix - every node only gets a part of it (subset of rows)
    private Matrix X;


    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        /* load cooc matrix */
        dataManager.loadAsMatrix(INPUT_DATA, NUM_WORDS_IN_COOC_MATRIX, NUM_WORDS_IN_COOC_MATRIX,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD));

        /* create matrices */
        W = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
        W_old = W.copy();

        GradSq = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
        GradSq_old = GradSq.copy();

        /* create bias vectors */
        GradSqB = new VectorBuilder()
                .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();
        GradSqB_old = GradSqB.copy();

        B = new VectorBuilder()
                .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();
        B_old = B.copy();

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

        /* register pull request handlers  */
        if (USE_PULL_REQUEST_FEATURE) {
            dataManager.registerPullRequestHandler("WPullRequest", new MatrixPullRequestHandler(W, W_old));
            dataManager.registerPullRequestHandler("GradSqPullRequest", new MatrixPullRequestHandler(GradSq, GradSq_old));
            dataManager.registerPullRequestHandler("BPullRequest", new VectorPullRequestHandler(B, B_old));
            dataManager.registerPullRequestHandler("GradSqBPullRequest", new VectorPullRequestHandler(GradSqB, GradSqB_old));
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

        int offset = 0; //TODO offset per node: offset = NUM_WORDS_IN_COOC_MATRIX / num_instances * ctx.instanceID;

        int iterations = 0;

        while (iterations++ < MAX_ITER /* TODO: abort on convergence criteria ? */) {

            LOG.info("Starting iteration " + (iterations - 1));

            double costI = 0;

            for (int ridxLocal = 0; ridxLocal < X.numRows(); ridxLocal++) {
                for (int cidx = 0; cidx < NUM_WORDS_IN_COOC_MATRIX; cidx++) {

                    int wordVecIdx = offset + ridxLocal;
                    int ctxVecIdx = cidx + NUM_WORDS_IN_COOC_MATRIX;

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

                    /*if (ridxLocal == 0 && cidx < 4) {
                        LOG.info("Diff: " + diff);
                        LOG.info("Fdiff: " + fdiff);
                        LOG.info("Cost: " + costI);
                        LOG.info("w1: " + w1.toString());
                        LOG.info("w2: " + w2.toString());
                        LOG.info("X: " + xVal);
                    }*/

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

                    /*if (ridxLocal == 0 && cidx < 4) {
                        LOG.info("w1 nach upd: " + W.colAsVector(wordVecIdx));
                        LOG.info("w2 nach upd: " + W.colAsVector(ctxVecIdx));
                        LOG.info("b1 nach upd: " + B.get(wordVecIdx));
                        LOG.info("b2 nach upd: " + B.get(ctxVecIdx));
                        LOG.info("gradsq nach upd: " + GradSq.colAsVector(wordVecIdx));
                        LOG.info("gradsq w2 nach upd: " + GradSq.colAsVector(ctxVecIdx));
                        LOG.info("gradsqB nach upd: " + GradSqB.get(wordVecIdx));
                        LOG.info("gradsq b2 nach upd: " + GradSqB.get(ctxVecIdx));
                    }*/

                }
            }

            costI /= X.numRows() * NUM_WORDS_IN_COOC_MATRIX;
            LOG.info("Iteration, Cost: " + (iterations - 1) + ", " + costI);

            /* pull data from all other nodes after each iteration */
            // TODO: intelligent pull - wait for Tobias' "pullRequest" impl with the "preprocessing on sending nodes" feature

            // TODO HAVE A LOOK AT PullRequestPrimitiveTestJob in examples/playground, I HOPE THIS IS THE PRIMITIVE YOU NEED!

            if (USE_PULL_REQUEST_FEATURE) {
                performPullRequest(W, "WPullRequest");
                W_old = W.copy();
                performPullRequest(GradSq, "GradSqPullRequest");
                GradSq_old = GradSq.copy();
                performPullRequest(B, "BPullRequest");
                B_old = B.copy();
                performPullRequest(GradSqB, "GradSqBPullRequest");
                GradSqB_old = GradSqB.copy();
            } else {
                dataManager.pullMerge(W, matrixMerger);
                dataManager.pullMerge(GradSq, matrixMerger);
                dataManager.pullMerge(B, vectorMerger);
                dataManager.pullMerge(GradSqB, vectorMerger);
            }
        }
    }

    // ---------------------------------------------------
    // Pull Request Stuff.
    // ---------------------------------------------------

    private class MatrixPullRequestHandler implements DataManager.PullRequestHandler {

        private Matrix m;
        private Matrix m_old;

        public MatrixPullRequestHandler(Matrix m, Matrix m_old) {
            this.m = m;
            this.m_old = m_old;
        }

        @Override
        public Object handlePullRequest(String name) {
            Matrix diffMatrix = new MatrixBuilder()
                    .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                    .format(Matrix.Format.SPARSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .build();
            iterateMatrix(m, (row, col, val) -> {
                if (Math.abs(m.get(row, col) - m_old.get(row, col)) > MATRIX_TRANSMIT_THRESHOLD) {
                    diffMatrix.set(row, col, m.get(row, col));
                }
            });
            return diffMatrix;
        }
    }

    private class VectorPullRequestHandler implements DataManager.PullRequestHandler {

        private Vector v;
        private Vector v_old;

        public VectorPullRequestHandler(Vector v, Vector v_old) {
            this.v = v;
            this.v_old = v_old;
        }

        @Override
        public Object handlePullRequest(String name) {
            Vector diffVector = new VectorBuilder()
                    .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                    .format(Vector.Format.SPARSE_VECTOR)
                    .layout(Vector.Layout.COLUMN_LAYOUT)
                    .build();
            Iterator<Vector.Element> elementIterator = v.iterateNonZero();
            while(elementIterator.hasNext()) {
                Vector.Element element = elementIterator.next();
                int col = element.index();
                if (Math.abs(v.get(col) - v_old.get(col)) > MATRIX_TRANSMIT_THRESHOLD) {
                    diffVector.set(col, v.get(col));
                }
            }
            return diffVector;
        }
    }

    private void performPullRequest(Matrix m, String pullRequestName) {
        Object[] m_diffs = dataManager.pullRequest(pullRequestName);
        Matrix diffCounts = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Matrix.Format.SPARSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
        for (Object _diff : m_diffs) {
            Matrix diff = (Matrix) _diff;
            iterateMatrix(m, (row, col, val) -> {
                m.set(row, col, diff.get(row, col));
                diffCounts.set(row, col, diffCounts.get(row, col) + 1);
            });
        }
        m.applyOnElements(diffCounts, (w, d) -> w / (d + 1));
    }

    private void performPullRequest(Vector v, String pullRequestName) {
        Object[] v_diffs = dataManager.pullRequest(pullRequestName);
        Vector diffCounts = new VectorBuilder()
                .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Vector.Format.SPARSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();
        for (Object _diff : v_diffs) {
            Vector diff = (Vector) _diff;
            Iterator<Vector.Element> elementIterator = v.iterateNonZero();
            while(elementIterator.hasNext()) {
                Vector.Element element = elementIterator.next();
                int col = element.index();
                v.set(col, diff.get(col));
                diffCounts.set(col, diffCounts.get(col) + 1);
            }
        }
        v.applyOnElements(diffCounts, (w, d) -> w / (d + 1));
    }

    private static void iterateMatrix(Matrix m, MatrixIterFunctionArg arg) {
        Matrix.RowIterator rowIterator = m.rowIterator();
        int row = 0;
        while(rowIterator.hasNextRow()) {
            Iterator<Vector.Element> elementIterator = rowIterator.getAsVector().iterateNonZero();
            while(elementIterator.hasNext()) {
                Vector.Element element = elementIterator.next();
                int col = element.index();
                arg.operation(row, col, m.get(row, col));
            }
            row++;
        }
    }

    private interface MatrixIterFunctionArg {
        void operation(int row, int col, double val);
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