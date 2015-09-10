package de.tuberlin.pserver.examples.experiments.glove;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.iteration.Loop;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.*;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.state.filter.MatrixUpdateFilter;
import de.tuberlin.pserver.runtime.state.merger.MatrixUpdateMerger;
import de.tuberlin.pserver.runtime.state.merger.VectorUpdateMerger;
import de.tuberlin.pserver.types.PartitionType;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Random;

public final class GloVeJobAdaGradPull extends MLProgram {

    // ---------------------------------------------------

    private static final int        ROWS = 50;

    private static final int        COLS = 36073;

    private static final double     ALPHA = 0.75;

    private static final int        X_MAX = 10;

    private static final double     LEARNING_RATE = 0.05;

    private static final int        NUM_EPOCHS = 15;

    // ---------------------------------------------------

    @SharedState(globalScope = GlobalScope.PARTITIONED, partitionType = PartitionType.ROW_PARTITIONED,
            rows = COLS, cols = COLS, path = "datasets/text8_coocc.csv")
    public Matrix X;

    @SharedState(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = COLS * 2, remoteUpdate = RemoteUpdate.DELTA_MERGE_UPDATE)
    public Matrix W;

    @SharedState(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = COLS * 2, remoteUpdate = RemoteUpdate.SIMPLE_MERGE_UPDATE)
    public Matrix GradSq;

    @SharedState(globalScope = GlobalScope.REPLICATED, cols = COLS * 2, remoteUpdate = RemoteUpdate.SIMPLE_MERGE_UPDATE)
    public Vector B;

    @SharedState(globalScope = GlobalScope.REPLICATED, cols = COLS * 2, remoteUpdate = RemoteUpdate.SIMPLE_MERGE_UPDATE)
    public Vector GradSqB;

    // ---------------------------------------------------

    @StateExtractor(stateObjects = "W, GradSq")
    public final MatrixUpdateFilter deltaFilter = (i, j, o, n) -> {
        final double sn = n * 10000;
        final double so = o * 10000;
        final double d = sn > 0 ? sn : 1;
        return (((so - sn) / d) > 0.2);
    };

    // ---------------------------------------------------

    @StateMerger(stateObjects = "W, GradSq")
    public final MatrixUpdateMerger matrixMerger = (i, j, val, remoteVal) -> (val + remoteVal) / 2;

    // ---------------------------------------------------

    @StateMerger(stateObjects = "B, GradSqB")
    public final VectorUpdateMerger vectorMerger = (i, val, remoteVal) -> (val + remoteVal) / 2;

    // ---------------------------------------------------

    @Override
    public void define(final Program program) {

        program.initialize(() -> {

            final Random rand = new Random();
            for (int i = 0; i < W.rows(); ++i) {
                for (int j = 0; j < W.cols(); ++j) {
                    W.set(i, j, (rand.nextDouble() - 0.5) / ROWS);
                    GradSq.set(i, j, 1.0);
                }
            }

            for (int i = 0; i < GradSqB.length(); i++) {
                B.set(i, (rand.nextDouble() - 0.5) / ROWS);
                GradSqB.set(i, 1.0);
            }

        }).process(() -> {

            CF.loop().sync(Loop.ASYNC).exe(NUM_EPOCHS, (e0) -> {

                final MutableDouble costI = new MutableDouble(0.0);

                LOG.info("Epoch = " + e0);

                CF.loop().parExe(X, (e, wordVecIdx, j, v) -> {

                    final long ctxVecIdx = j + COLS;

                    if (v == 0)
                        return;

                    final Vector w1 = W.colAsVector(wordVecIdx);
                    final double b1 = B.get(wordVecIdx);
                    final Vector gs1 = GradSq.colAsVector(wordVecIdx);
                    final Vector w2 = W.colAsVector(ctxVecIdx);
                    final double b2 = B.get(ctxVecIdx);
                    final Vector gs2 = GradSq.colAsVector(ctxVecIdx);

                    final double diff = w1.dot(w2) + b1 + b2 - Math.log(v);
                    double fdiff = (v > X_MAX) ? diff : Math.pow(v / X_MAX, ALPHA) * diff;

                    costI.add(0.5 * diff * fdiff);

                    fdiff *= LEARNING_RATE;

                    final Vector grad1 = w2.mul(fdiff);
                    final Vector grad2 = w1.mul(fdiff);

                    W.assignColumn(wordVecIdx, w1.add(-1, grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(ctxVecIdx, w2.add(-1, grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(wordVecIdx, b1 - fdiff / Math.sqrt(GradSqB.get(wordVecIdx)));
                    B.set(ctxVecIdx, b2 - fdiff / Math.sqrt(GradSqB.get(ctxVecIdx)));

                    gs1.assign(gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2));
                    gs2.assign(gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2));

                    GradSq.assignColumn(wordVecIdx, gs1);
                    GradSq.assignColumn(ctxVecIdx, gs2);

                    GradSqB.set(wordVecIdx, GradSqB.get(wordVecIdx) + fdiff * fdiff);
                    GradSqB.set(ctxVecIdx, GradSqB.get(ctxVecIdx) + fdiff * fdiff);
                });

                DF.publishUpdate();
                DF.pullUpdate();
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(GloVeJobAdaGradPull.class, 1)
                .done();
    }
}