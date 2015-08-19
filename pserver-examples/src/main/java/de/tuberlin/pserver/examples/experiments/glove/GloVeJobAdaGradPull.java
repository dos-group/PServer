package de.tuberlin.pserver.examples.experiments.glove;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.DeltaFilter;
import de.tuberlin.pserver.dsl.state.DeltaUpdate;
import de.tuberlin.pserver.dsl.state.GlobalScope;
import de.tuberlin.pserver.dsl.state.State;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaFilter;
import de.tuberlin.pserver.types.PartitionType;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Random;

public class GloVeJobAdaGradPull extends MLProgram {

    // ---------------------------------------------------

    private static final int        ROWS = 50;

    private static final int        COLS = 36073;

    private static final double     ALPHA = 0.75;

    private static final int        X_MAX = 10;

    private static final double     LEARNING_RATE = 0.05;

    private static final int        NUM_EPOCHS = 15;

    // ---------------------------------------------------

    @State(globalScope = GlobalScope.PARTITIONED, partitionType = PartitionType.ROW_PARTITIONED, rows = COLS, cols = COLS,
            path = "datasets/text8_coocc.csv", format = Format.SPARSE_FORMAT)
    public Matrix X;

    @State(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = COLS * 2, delta = DeltaUpdate.LZ4_DELTA)
    public Matrix W;

    @State(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix GradSq;

    @State(globalScope = GlobalScope.REPLICATED, cols = COLS * 2)
    public Vector B;

    @State(globalScope = GlobalScope.REPLICATED, cols = COLS * 2)
    public Vector GradSqB;

    // ---------------------------------------------------

    @DeltaFilter(stateObjects = "W")
    public final MatrixDeltaFilter W_deltaFilter = (i, j, o, n) -> {
        final double sn = n * 10000;
        final double so = o * 10000;
        final double d = sn > 0 ? sn : 1;
        return (((so - sn) / d) > 0.3);
    };

    // ---------------------------------------------------

    private static final DataManager.Merger<Vector> vectorMerger = (dst, src) -> {
        for (final Vector b : src)
            dst.applyOnElements(b, (e1, e2) -> e1 + e2, dst);
        dst.applyOnElements(e -> e / (src.size() + 1), dst);
    };

    private static final DataManager.Merger<Matrix> matrixMerger = (dst, src) -> {
        for (final Matrix m : src)
            dst.applyOnElements(m, (e1, e2) -> e1 + e2);
        dst.applyOnElements(e -> e / (src.size() + 1));
    };

    // ---------------------------------------------------

    @Override
    public void define(final Program program) {

        program.initialize(() -> {

            final Random rand = new Random();
            for (int i = 0; i < W.numRows(); ++i) {
                for (int j = 0; j < W.numCols(); ++j) {
                    W.set(i, j, (rand.nextDouble() - 0.5) / ROWS);
                    GradSq.set(i, j, 1.0);
                }
            }

            for (int i = 0; i < GradSqB.length(); i++) {
                B.set(i, (rand.nextDouble() - 0.5) / ROWS);
                GradSqB.set(i, 1.0);
            }

        }).process(() -> {

            CF.iterate().exe(NUM_EPOCHS, (e0) -> {

                final MutableDouble costI = new MutableDouble(0.0);

                CF.syncSlots();

                LOG.info("Epoch = " + e0);

                CF.iterate().parExe(X, (e, i, j, v) -> {

                    long wordVecIdx = i;
                    long ctxVecIdx = j + COLS;
                    double xVal = v;

                    if (xVal == 0)
                        return;

                    Vector w1 = W.colAsVector(wordVecIdx);
                    double b1 = B.get(wordVecIdx);
                    Vector gs1 = GradSq.colAsVector(wordVecIdx);
                    Vector w2 = W.colAsVector(ctxVecIdx);
                    double b2 = B.get(ctxVecIdx);
                    Vector gs2 = GradSq.colAsVector(ctxVecIdx);

                    double diff = w1.dot(w2) + b1 + b2 - Math.log(xVal);
                    double fdiff = (xVal > X_MAX) ? diff : Math.pow(xVal / X_MAX, ALPHA) * diff;

                    costI.add(0.5 * diff * fdiff);

                    fdiff *= LEARNING_RATE;

                    Vector grad1 = w2.mul(fdiff);
                    Vector grad2 = w1.mul(fdiff);

                    W.assignColumn(wordVecIdx, w1.add(-1, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(ctxVecIdx, w2.add(-1, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(wordVecIdx, b1 - fdiff / Math.sqrt(GradSqB.get(wordVecIdx)));
                    B.set(ctxVecIdx, b2 - fdiff / Math.sqrt(GradSqB.get(ctxVecIdx)));

                    gs1 = gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2);
                    gs2 = gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2);

                    GradSq.assignColumn(wordVecIdx, gs1);
                    GradSq.assignColumn(ctxVecIdx, gs2);

                    GradSqB.set(wordVecIdx, GradSqB.get(wordVecIdx) + fdiff * fdiff);
                    GradSqB.set(ctxVecIdx, GradSqB.get(ctxVecIdx) + fdiff * fdiff);
                });

                slotContext.CF.syncSlots();

                slotContext.DF.computeDelta("W");

                CF.select()
                        .slot(0)
                        .exe(() -> {
                            //dataManager.pullMerge(W, matrixMerger);
                            //dataManager.pullMerge(GradSq, matrixMerger);
                            //dataManager.pullMerge(B, vectorMerger);
                            //dataManager.pullMerge(GradSqB, vectorMerger);
                        });
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(GloVeJobAdaGradPull.class, 8)
                .done();
    }
}