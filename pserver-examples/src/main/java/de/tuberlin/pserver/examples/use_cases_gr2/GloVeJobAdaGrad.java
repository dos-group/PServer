package de.tuberlin.pserver.examples.use_cases_gr2;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.*;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GloVeJobAdaGrad extends PServerJob {

    private enum DataExchangeMode {
        DELTA_PUSH, PUSH, DELTA_PULL, PULL
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final DataExchangeMode dataExchangeMode = DataExchangeMode.PULL;

    /* input data parameter */
    private static final int NUM_WORDS_IN_COOC_MATRIX = 1000;
    private static final String INPUT_DATA = "/data/home/fgoessler/cooc-agg.csv";
    private static final String OUTPUT_DATA = "/data/home/fgoessler/pserver_glove.csv";

    /* hyperparameter */
    private static final int VEC_DIM = 50;
    private static final double ALPHA = 0.75;
    private static final int XMAX = 10;
    private static final double LearningRate = 0.05;
    private static final int MAX_ITER = 15;
    private static final double MATRIX_TRANSMIT_THRESHOLD = 0.0;

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

        /* load cooc matrix */
        dataManager.loadAsMatrix(INPUT_DATA, NUM_WORDS_IN_COOC_MATRIX, NUM_WORDS_IN_COOC_MATRIX,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD),
                Matrix.Format.SPARSE_MATRIX, Matrix.Layout.ROW_LAYOUT);

        /* create matrices */
        W = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
        W_old = W.copy();
        W_deltas = W.copy();        //TODO: use sparse matrix

        GradSq = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
        GradSq_old = GradSq.copy();
        GradSq_deltas = GradSq.copy();      //TODO: use sparse matrix

        /* create bias vectors */
        GradSqB = new VectorBuilder()
                .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();
        GradSqB_old = GradSqB.copy();
        GradSqB_deltas = GradSqB.copy();    //TODO: use sparse vector

        B = new VectorBuilder()
                .dimension(NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();
        B_old = B.copy();
        B_deltas = B.copy();    //TODO: use sparse vector

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
        if (dataExchangeMode == DataExchangeMode.DELTA_PULL) {
            dataManager.registerPullRequestHandler("WPullRequest", new MatrixPullRequestHandler(W, W_old));
            dataManager.registerPullRequestHandler("GradSqPullRequest", new MatrixPullRequestHandler(GradSq, GradSq_old));
            dataManager.registerPullRequestHandler("BPullRequest", new VectorPullRequestHandler(B, B_old));
            dataManager.registerPullRequestHandler("GradSqBPullRequest", new VectorPullRequestHandler(GradSqB, GradSqB_old));
        }

        /* register push event handlers */
        if (dataExchangeMode == DataExchangeMode.PUSH) {
            W_lock = new ReentrantLock();
            dataManager.addDataEventListener("W", new MatrixPushEventHandler(W, W_lock));
            B_lock = new ReentrantLock();
            dataManager.addDataEventListener("B", new VectorPushEventHandler(B, B_lock));
            GradSq_lock = new ReentrantLock();
            dataManager.addDataEventListener("GradSq", new MatrixPushEventHandler(GradSq, GradSq_lock));
            GradSqB_lock = new ReentrantLock();
            dataManager.addDataEventListener("GradSqB", new VectorPushEventHandler(GradSqB, GradSqB_lock));
        }

        /* register delta-push event handlers */
        if (dataExchangeMode == DataExchangeMode.DELTA_PUSH) {
            W_lock = new ReentrantLock();
            dataManager.addDataEventListener("W", new MatrixDeltaPushEventHandler(W, W_lock));
            B_lock = new ReentrantLock();
            dataManager.addDataEventListener("B", new VectorDeltaPushEventHandler(B, B_lock));
            GradSq_lock = new ReentrantLock();
            dataManager.addDataEventListener("GradSq", new MatrixDeltaPushEventHandler(GradSq, GradSq_lock));
            GradSqB_lock = new ReentrantLock();
            dataManager.addDataEventListener("GradSqB", new VectorDeltaPushEventHandler(GradSqB, GradSqB_lock));
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

        int numInstances = dataManager.getNumberOfInstances();
        int offset = NUM_WORDS_IN_COOC_MATRIX / numInstances * ctx.instanceID;

        int iterations = 0;

        while (iterations++ < MAX_ITER /* TODO: abort on convergence criteria ? */) {

            LOG.info("Starting iteration " + (iterations - 1));

            double costI = 0;

            for (int ridxLocal = 0; ridxLocal < X.numRows(); ridxLocal++) {
                for (int cidx = 0; cidx < NUM_WORDS_IN_COOC_MATRIX; cidx++) {

                    /* lock all matrices to avoid conflicts with incoming pushes */
                    // TODO: locking & unlocking for every step might be an performance issue
                    if (dataExchangeMode == DataExchangeMode.PUSH || dataExchangeMode == DataExchangeMode.DELTA_PUSH) {
                        W_lock.lock();
                        B_lock.lock();
                        GradSq_lock.lock();
                        GradSqB_lock.lock();   
                    }

                    
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

                    if (dataExchangeMode == DataExchangeMode.DELTA_PUSH) {
                        applyOnColumnVectorAndSetDeltaFlagIfSignificant(W, W_deltas, wordVecIdx, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2)), (v1, v2) -> v1 * -1 + v2);
                        applyOnColumnVectorAndSetDeltaFlagIfSignificant(W, W_deltas, ctxVecIdx, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2)), (v1, v2) -> v1 * -1 + v2);

                        applyOnIndexAndSetDeltaFlagIfSignificant(B, B_deltas, wordVecIdx, fdiff, (v, _fdiff) -> v - _fdiff / Math.sqrt(GradSqB.get(wordVecIdx)));
                        applyOnIndexAndSetDeltaFlagIfSignificant(B, B_deltas, ctxVecIdx, fdiff, (v, _fdiff) -> v - _fdiff / Math.sqrt(GradSqB.get(ctxVecIdx)));

                        /* update gradient adjustments for AdaGrad */
                        applyOnColumnVectorAndSetDeltaFlagIfSignificant(GradSq, GradSq_deltas, wordVecIdx, grad1, (el1, el2) -> el1 + el2 * el2);
                        applyOnColumnVectorAndSetDeltaFlagIfSignificant(GradSq, GradSq_deltas, ctxVecIdx, grad2, (el1, el2) -> el1 + el2 * el2);

                        applyOnIndexAndSetDeltaFlagIfSignificant(GradSqB, GradSqB_deltas, wordVecIdx, fdiff, (v, _fdiff) -> v + _fdiff * _fdiff);
                        applyOnIndexAndSetDeltaFlagIfSignificant(GradSqB, GradSqB_deltas, ctxVecIdx, fdiff, (v, _fdiff) -> v + _fdiff * _fdiff);
                    } else {
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

                    /* unlock all matrices to allow incoming pushes */
                    if (dataExchangeMode == DataExchangeMode.PUSH || dataExchangeMode == DataExchangeMode.DELTA_PUSH) {
                        W_lock.unlock();
                        B_lock.unlock();
                        GradSq_lock.unlock();
                        GradSqB_lock.unlock();
                    }
                    
                }
            }

            costI /= X.numRows() * NUM_WORDS_IN_COOC_MATRIX;
            LOG.info("Iteration, Cost: " + (iterations - 1) + ", " + costI);

            /* merge data from/ with other nodes: implemented 3 variants yet: push, pull and delta-pull */
            if (dataExchangeMode == DataExchangeMode.DELTA_PULL) {
                /* pull data from all other nodes after each iteration and allow them to preprocess the data to only send diffs */
                // TODO: resetting *_old after each pull might be problematic cause it's possible that not all or even
                // no other node has pulled the significant deltas from this node and so they will get lost
                performPullRequest(W, "WPullRequest");
                W_old = W.copy();
                performPullRequest(GradSq, "GradSqPullRequest");
                GradSq_old = GradSq.copy();
                performPullRequest(B, "BPullRequest");
                B_old = B.copy();
                performPullRequest(GradSqB, "GradSqBPullRequest");
                GradSqB_old = GradSqB.copy();
            } if (dataExchangeMode == DataExchangeMode.PUSH) {
                /* push the calculations of this node to all other nodes */
                dataManager.pushTo("W", W);
                dataManager.pushTo("GradSq", GradSq);
                dataManager.pushTo("B", B);
                dataManager.pushTo("GradSqB", GradSqB);
            } else if (dataExchangeMode == DataExchangeMode.DELTA_PUSH) {
                /* push the significant calculations of this node to all other nodes */
                pushDeltaMatrix("W", W, W_deltas);
                pushDeltaMatrix("GradSq", GradSq, GradSq_deltas);
                pushDeltaVector("B", B, B_deltas);
                pushDeltaVector("GradSqB", GradSqB, GradSqB_deltas);
            } else {
                /* pull data from all other nodes after each iteration */
                dataManager.pullMerge(W, matrixMerger);
                dataManager.pullMerge(GradSq, matrixMerger);
                dataManager.pullMerge(B, vectorMerger);
                dataManager.pullMerge(GradSqB, vectorMerger);
            }
        }

        result(W);
    }

    // ---------------------------------------------------
    // Delta Push Stuff.
    // ---------------------------------------------------

    private void pushDeltaMatrix(String name, Matrix m, Matrix significantDeltas) {
        // significantDeltas should only contain 0 or 1
        significantDeltas.applyOnElements(m, (v1, v2) -> v1 * v2);
        dataManager.pushTo(name, significantDeltas);
        // reset delta matrix
        significantDeltas.applyOnElements((v1) -> 0);   // TODO: this can be optimized...
    }

    private void pushDeltaVector(String name, Vector v, Vector significantDeltas) {
        // significantDeltas should only contain 0 or 1
        significantDeltas.applyOnElements(v, (v1, v2) -> v1 * v2);
        dataManager.pushTo(name, significantDeltas);
        // reset delta matrix
        significantDeltas.applyOnElements((v1) -> 0);   // TODO: this can be optimized...
    }

    private void applyOnColumnVectorAndSetDeltaFlagIfSignificant(Matrix m, Matrix deltas, Long col, Vector v, Vector.VectorFunction2Arg func) {
        for (int i = 0; i < m.numRows(); i++) {
            double oldVal = m.get(i, col);
            double newVal = func.operation(oldVal, v.get(i));
            if (Math.abs(newVal - oldVal) > MATRIX_TRANSMIT_THRESHOLD) {
                deltas.set(i, col, 1);
            }
            m.set(i, col, newVal);
        }
    }

    private void applyOnIndexAndSetDeltaFlagIfSignificant(Vector v, Vector deltas, long idx, double val, Vector.VectorFunction2Arg func) {
        double oldVal = v.get(idx);
        double newVal = func.operation(oldVal, val);
        if (Math.abs(newVal - oldVal) > MATRIX_TRANSMIT_THRESHOLD) {
            deltas.set(idx, 1);
        }
        v.set(idx, newVal);
    }

    private class MatrixDeltaPushEventHandler extends DataManager.DataEventHandler {

        private Matrix m;
        private Lock lock;

        public MatrixDeltaPushEventHandler(Matrix m, Lock lock) {
            this.m = m;
            this.lock = lock;
        }

        @Override
        public void handleDataEvent(int srcInstanceID, Object value) {
            lock.lock();
            m.applyOnElements((Matrix) value, (v1, v2) -> { //TODO: optimize to use advantage of sparse matrix
                if(v2 > 0.0) {      // sending a 0 in the delta matrix means no significant change
                    return (v1 + v2) / 2.0;
                } else {
                    return v1;
                }
            });
            //TODO: update local significant change matrix?
            lock.unlock();
        }
    }

    private class VectorDeltaPushEventHandler extends DataManager.DataEventHandler {

        private Vector v;
        private Lock lock;

        public VectorDeltaPushEventHandler(Vector v, Lock lock) {
            this.v = v;
            this.lock = lock;
        }

        @Override
        public void handleDataEvent(int srcInstanceID, Object value) {
            lock.lock();
            v.applyOnElements((Vector) value, (v1, v2) -> { //TODO: optimize to use advantage of sparse vector
                if(v2 > 0.0) {      // sending a 0 in the delta matrix means no significant change
                    return (v1 + v2) / 2.0;
                } else {
                    return v1;
                }
            });
            //TODO: update local significant change vector?
            lock.unlock();
        }
    }

    // ---------------------------------------------------
    // Push Stuff.
    // ---------------------------------------------------

    private class MatrixPushEventHandler extends DataManager.DataEventHandler {

        private Matrix m;
        private Lock lock;

        public MatrixPushEventHandler(Matrix m, Lock lock) {
            this.m = m;
            this.lock = lock;
        }

        @Override
        public void handleDataEvent(int srcInstanceID, Object value) {
            lock.lock();
            m.applyOnElements((Matrix) value, (v1, v2) -> (v1 + v2) / 2.0);
            lock.unlock();
        }
    }

    private class VectorPushEventHandler extends DataManager.DataEventHandler {

        private Vector v;
        private Lock lock;

        public VectorPushEventHandler(Vector v, Lock lock) {
            this.v = v;
            this.lock = lock;
        }

        @Override
        public void handleDataEvent(int srcInstanceID, Object value) {
            lock.lock();
            v.applyOnElements((Vector) value, (v1, v2) -> (v1 + v2) / 2.0);
            lock.unlock();
        }
    }


    // ---------------------------------------------------
    // Delta Pull aka Pull Request Stuff.
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
            rowIterator.nextRow();
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
    // Pull Stuff.
    // ---------------------------------------------------

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
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(GloVeJobAdaGrad.class)
                .results(res)
                .done();

        printMatrix(mergeMatrices(res), OUTPUT_DATA);
    }

    // Prints the matrix as a csv file. Each line is formatted as "<row>;<col>;<value>".
    private static void printMatrix(Matrix w_avg, String fileName) {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(fileName, "UTF-8");
            for (int i = 0; i < w_avg.numRows(); ++i) {
                for (int j = 0; j < w_avg.numCols(); ++j) {
                    writer.println(i + ";" + j + ";" + w_avg.get(i, j));
                }
            }
        } catch (Exception ignored) {
        } finally {
            if(writer != null) writer.close();
        }
    }

    private static Matrix mergeMatrices(List<List<Serializable>> res) {
        Matrix W_avg = new MatrixBuilder()
                .dimension(VEC_DIM, NUM_WORDS_IN_COOC_MATRIX * 2)
                .format(Matrix.Format.DENSE_MATRIX)
                .layout(Matrix.Layout.ROW_LAYOUT)
                .build();
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
}