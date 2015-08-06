package de.tuberlin.pserver.examples.use_cases_gr2;

import de.tuberlin.pserver.app.DataManager;
import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.app.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.VectorBuilder;

import java.util.Random;

public class GloVeJobAdaGradPull_MC extends PServerJob {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // Input-Parameter.
    private static final int        NUM_WORDS_IN_COOC_MATRIX = 36073;

    private static final String     INPUT_DATA = "datasets/text8_coocc.csv";

    // Hyper-Parameter.
    private static final int        VEC_DIM = 50;

    private static final double     ALPHA = 0.75;

    private static final int        XMAX = 10;

    private static final double     LEARNING_RATE = 0.05;

    private static final int        MAX_ITER = 15;

    // Weight Matrix (aka Model).
    private Matrix W;

    // Biases for the Weight Matrix.
    private Vector B;

    // Gradient Adjustments for AdaGrad.
    private Matrix GradSq;

    private Vector GradSqB;

    // Co-Occurrence Matrix - every node only gets a part of it (subset of rows).
    private Matrix X;

    // ---------------------------------------------------
    // Pull Stuff.
    // ---------------------------------------------------

    private static final DataManager.Merger<Vector> vectorMerger = (dst, src) -> {
        for (final Vector b : src) {
            dst.assign(b, (e1, e2) -> e1 + e2);
        }

        dst.assign(e -> e / (src.size() + 1));
    };

    private static final DataManager.Merger<Matrix> matrixMerger = (dst, src) -> {
        for (final Matrix m : src) {
            dst.applyOnElements(m, (e1, e2) -> e1 + e2);
        }

        dst.applyOnElements(e -> e / (src.size() + 1));
    };

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        // Create Matrices.
        W       = createMatrix(Matrix.Format.DENSE_MATRIX);
        GradSq  = createMatrix(Matrix.Format.DENSE_MATRIX);

        // Create Bias Vectors.
        B       = createVector(Vector.Format.DENSE_VECTOR);
        GradSqB = createVector(Vector.Format.DENSE_VECTOR);

        // Initialize Matrices and Bias Vectors.
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

        // Load Training Data.
        dataManager.loadAsMatrix(
                INPUT_DATA,
                NUM_WORDS_IN_COOC_MATRIX,
                NUM_WORDS_IN_COOC_MATRIX,
                RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD),
                Matrix.Format.SPARSE_MATRIX,
                Matrix.Layout.ROW_LAYOUT
        );

        // Make Matrices and Vectors available for all nodes.
        dataManager.putObject("W", W);
        dataManager.putObject("GradSq", GradSq);
        dataManager.putObject("B", B);
        dataManager.putObject("GradSqB", GradSqB);
    }

    @Override
    public void compute() {

        X       = dataManager.getObject(INPUT_DATA);
        W       = dataManager.getObject("W");
        GradSq  = dataManager.getObject("GradSq");
        B       = dataManager.getObject("B");
        GradSqB = dataManager.getObject("GradSqB");

        int numInstances = instanceContext.jobContext.numOfNodes;

        int offset = (NUM_WORDS_IN_COOC_MATRIX / numInstances * instanceContext.jobContext.nodeID)
                + (NUM_WORDS_IN_COOC_MATRIX / numInstances / instanceContext.jobContext.numOfInstances) * instanceContext.instanceID;

        final Matrix.RowIterator xIter = dataManager.createThreadPartitionedRowIterator(X);

        int epoch = 0;

        while (epoch++ < MAX_ITER) {

            LOG.info("Starting iteration " + (epoch - 1));

            double costI = 0;

            while(xIter.hasNextRow()) {

                xIter.nextRow();

                for (int col = 0; col < NUM_WORDS_IN_COOC_MATRIX; col++) {

                    long wordVecIdx = offset + xIter.getCurrentRowNum();
                    long ctxVecIdx = col + NUM_WORDS_IN_COOC_MATRIX;

                    Double xVal = xIter.getValueOfColumn(col); //X.get(row, col);

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

                    // calculate gradient
                    double diff = w1.dot(w2) + b1 + b2 - Math.log(xVal);
                    double fdiff = (xVal > XMAX) ? diff : Math.pow(xVal / XMAX, ALPHA) * diff;

                    // GLOBAL OPERATION - EXCHANGE cost
                    costI += 0.5 * diff * fdiff;

                    fdiff *= LEARNING_RATE;

                    Vector grad1 = w2.mul(fdiff);
                    Vector grad2 = w1.mul(fdiff);

                    W.assignColumn(wordVecIdx, w1.add(-1, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(ctxVecIdx, w2.add(-1, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(wordVecIdx, b1 - fdiff / Math.sqrt(GradSqB.get(wordVecIdx)));
                    B.set(ctxVecIdx, b2 - fdiff / Math.sqrt(GradSqB.get(ctxVecIdx)));

                    // Update Gradient Adjustments for AdaGrad.
                    gs1 = gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2);
                    gs2 = gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2);

                    GradSq.assignColumn(wordVecIdx, gs1);
                    GradSq.assignColumn(ctxVecIdx, gs2);

                    GradSqB.set(wordVecIdx, GradSqB.get(wordVecIdx) + fdiff * fdiff);
                    GradSqB.set(ctxVecIdx, GradSqB.get(ctxVecIdx) + fdiff * fdiff);
                }
            }

            costI /= X.numRows() * NUM_WORDS_IN_COOC_MATRIX;
            LOG.info("Iteration, Cost: " + (epoch - 1) + ", " + costI);

            // pull data from all other nodes after each iteration
            if (instanceContext.instanceID == 0) {
                dataManager.pullMerge(W, matrixMerger);
                dataManager.pullMerge(GradSq, matrixMerger);
                dataManager.pullMerge(B, vectorMerger);
                dataManager.pullMerge(GradSqB, vectorMerger);
            }

            xIter.reset();
        }
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

        PServerExecutor.LOCAL
                .run(GloVeJobAdaGradPull_MC.class, 4)
                .done();
    }
}