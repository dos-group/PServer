package de.tuberlin.pserver.examples.experiments.glove;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.mcruntime.Parallel;
import de.tuberlin.pserver.types.PartitionType;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Random;

public final class GloVeJobAdaGradPull extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int        ROWS = 50;

    private static final int        COLS = 36073;

    private static final double     ALPHA = 0.75;

    private static final int        X_MAX = 10;

    private static final double     LEARNING_RATE = 0.05;

    private static final int        NUM_EPOCHS = 15;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(globalScope = GlobalScope.PARTITIONED,
            rows = COLS, cols = COLS, path = "datasets/text8_coocc.csv")
    public Matrix X;

    @State(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix W;

    @State(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix GradSq;

    @State(globalScope = GlobalScope.REPLICATED, rows = 1, cols = COLS * 2, layout = Layout.COLUMN_LAYOUT)
    public Matrix B;

    @State(globalScope = GlobalScope.REPLICATED, rows = 1, cols = COLS * 2, layout = Layout.COLUMN_LAYOUT)
    public Matrix GradSqB;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(

            (Apply<Matrix, Void>) (updates) -> {
                for (final Matrix update : updates)
                    Parallel.For(update, (i, j, v) -> W.set(i, j, (W.get(i, j) + update.get(i, j)) / 2));
                return null;
            }
    );

    @Transaction(state = "GradSq", type = TransactionType.PULL)
    public final TransactionDefinition syncGradSq = new TransactionDefinition(

            (Apply<Matrix, Void>) (updates) -> {
                for (final Matrix update : updates)
                    Parallel.For(update, (i, j, v) -> GradSq.set(i, j, (GradSq.get(i, j) + update.get(i, j)) / 2));
                return null;
            }
    );

    @Transaction(state = "B", type = TransactionType.PULL)
    public final TransactionDefinition syncB = new TransactionDefinition(

            (Apply<Matrix, Void>) (updates) -> {
                for (final Matrix update : updates)
                    Parallel.For(update, (i, j, v) -> B.set(i, j, (B.get(i, j) + update.get(i, j)) / 2));
                return null;
            }
    );

    @Transaction(state = "GradSqB", type = TransactionType.PULL)
    public final TransactionDefinition syncGradSqB = new TransactionDefinition(

            (Apply<Matrix, Void>) (updates) -> {
                for (final Matrix update : updates)
                    Parallel.For(update, (i, j, v) -> GradSqB.set(i, j, (GradSqB.get(i, j) + update.get(i, j)) / 2));
                return null;
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

            final Random rand = new Random();
            for (int i = 0; i < W.rows(); ++i) {
                for (int j = 0; j < W.cols(); ++j) {
                    W.set(i, j, (rand.nextDouble() - 0.5) / ROWS);
                    GradSq.set(i, j, 1.0);
                }
            }

            for (int i = 0; i < GradSqB.cols(); i++) {
                B.set(0, i, (rand.nextDouble() - 0.5) / ROWS);
                GradSqB.set(0, i, 1.0);
            }

        }).process(() -> {

            UnitMng.loop(NUM_EPOCHS, (e0) -> {

                final MutableDouble costI = new MutableDouble(0.0);

                LOG.info("Epoch = " + e0);

                Parallel.For(X, (wordVecIdx, j, v) -> {

                    final long ctxVecIdx = j + COLS;

                    if (v == 0)
                        return;

                    final Matrix w1 = W.getCol(wordVecIdx);
                    final double b1 = B.get(wordVecIdx);
                    final Matrix gs1 = GradSq.getCol(wordVecIdx);
                    final Matrix w2 = W.getCol(ctxVecIdx);
                    final double b2 = B.get(ctxVecIdx);
                    final Matrix gs2 = GradSq.getCol(ctxVecIdx);

                    final double diff = w1.dot(w2) + b1 + b2 - Math.log(v);
                    double fdiff = (v > X_MAX) ? diff : Math.pow(v / X_MAX, ALPHA) * diff;

                    costI.add(0.5 * diff * fdiff);

                    fdiff *= LEARNING_RATE;

                    final Matrix grad1 = w2.scale(fdiff);
                    final Matrix grad2 = w1.scale(fdiff);

                    W.assignColumn(wordVecIdx, w1.sub(grad1.applyOnElements(gs1, (el1, el2) -> el1 / Math.sqrt(el2))));
                    W.assignColumn(ctxVecIdx, w2.sub(grad2.applyOnElements(gs2, (el1, el2) -> el1 / Math.sqrt(el2))));

                    B.set(0, wordVecIdx, b1 - fdiff / Math.sqrt(GradSqB.get(0, wordVecIdx)));
                    B.set(0, ctxVecIdx, b2 - fdiff / Math.sqrt(GradSqB.get(0, ctxVecIdx)));

                    gs1.assign(gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2));
                    gs2.assign(gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2));

                    GradSq.assignColumn(wordVecIdx, gs1);
                    GradSq.assignColumn(ctxVecIdx, gs2);

                    GradSqB.set(0, wordVecIdx, GradSqB.get(0, wordVecIdx) + fdiff * fdiff);
                    GradSqB.set(0, ctxVecIdx, GradSqB.get(0, ctxVecIdx) + fdiff * fdiff);
                });

                TransactionMng.commit(syncW);
                TransactionMng.commit(syncGradSq);
                TransactionMng.commit(syncB);
                TransactionMng.commit(syncGradSqB);
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(GloVeJobAdaGradPull.class)
                .done();
    }
}