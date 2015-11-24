package de.tuberlin.pserver.examples.experiments.glove;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Random;

public final class GloVe extends Program {

    // ---------------------------------------------------
    // Measurements.
    // ---------------------------------------------------

    // 1496250 ms (Local: 4 Nodes with 8 Threads per Node) - 64F

    // 1541774 ms (Local: 4 Nodes with 2 Threads per Node) - 64F

    // 1597422 ms (Local: 4 Nodes with 8 Threads per Node) - 32F

    // 1245857 ms (Local: 4 Nodes with 8 Threads per Node) - 32F - 20.76min => Optimized Math!

    // 1072146 ms (Local: 4 Nodes with 8 Threads per Node) - 32F - 17.86min => Optimized Math!
    
    // 1318430
    
    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int        ROWS = 50;

    private static final int        COLS = 36073;

    private static final double     ALPHA = 0.75;

    private static final int        X_MAX = 10;

    private static final double     LEARNING_RATE = 0.05;

    private static final int        NUM_EPOCHS = 15;

    private static final String     INPUT_DATA = "datasets/text8_coocc.csv";

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED, rows = COLS, cols = COLS, path = INPUT_DATA)
    public Matrix64F X;

    @State(scope = Scope.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix64F W;

    @State(scope = Scope.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix64F GradSq;

    @State(scope = Scope.REPLICATED, rows = 1, cols = COLS * 2)
    public Matrix64F B;

    @State(scope = Scope.REPLICATED, rows = 1, cols = COLS * 2)
    public Matrix64F GradSqB;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    /*private ObjectSerializer serializer = ObjectSerializer.Factory.create(ObjectSerializer.SerializerType.KRYO_SERIALIZER);

    private Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);

    @Transaction(state = "W, GradSq, B, GradSqB", type = TransactionType.PULL)
    public final TransactionDefinition sync = new TransactionDefinition(

            (Prepare<Matrix64F, byte[]>) (remoteMatrix) -> compressor.compress(serializer.serialize(remoteMatrix))
            ,

            (Apply2<byte[],Matrix64F, Matrix64F>) (remoteUpdates, localState) -> {
                for (final byte[] update : remoteUpdates) {
                    final Matrix64F updateMtx = serializer.deserialize(compressor.decompress(update));
                    Parallel.For(updateMtx, (i, j, v) -> localState.set(i, j, (localState.get(i, j) + updateMtx.get(i, j)) / 2));
                }
            }
    );*/

    @Transaction(state = "W, GradSq, B, GradSqB", type = TransactionType.PULL)
    public final TransactionDefinition sync = new TransactionDefinition(

            (Update<Matrix64F>) (remoteUpdates, localState) -> {
                for (final Matrix64F update : remoteUpdates)
                    Parallel.For(update, (i, j, v) -> localState.set(i, j, (localState.get(i, j) + update.get(i, j)) / 2));
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

            final Random rand = new Random();
            W.applyOnElements(v -> (double)(rand.nextDouble() - 0.5) / ROWS);
            B.applyOnElements(v -> (double)(rand.nextDouble() - 0.5) / ROWS);
            GradSq.assign(1.0);
            GradSqB.assign(1.0);

        }).process(() ->

            UnitMng.loop(NUM_EPOCHS, (e0) -> {

                final MutableDouble costI = new MutableDouble(0.0);

                LOG.info("Epoch = " + e0 + " at Node " + programContext.nodeID);

                atomic(state(W, B, GradSq, GradSqB), () -> {
                    Parallel.For(X, (wordVecIdx, j, v) -> {
                        if (v == 0) return;

                        final long ctxVecIdx = j + COLS;
                        final Matrix64F w1 = W.getCol(wordVecIdx);
                        final double b1 = B.get(wordVecIdx);
                        final Matrix64F gs1 = GradSq.getCol(wordVecIdx);
                        final Matrix64F w2 = W.getCol(ctxVecIdx);
                        final double b2 = B.get(ctxVecIdx);
                        final Matrix64F gs2 = GradSq.getCol(ctxVecIdx);

                        final double diff = w1.dot(w2) + b1 + b2 - (double) Math.log(v);
                        double fdiff = (v > X_MAX) ? diff : (double) Math.pow(v / X_MAX, ALPHA) * diff;

                        costI.add(0.5 * diff * fdiff);

                        fdiff *= LEARNING_RATE;

                        final Matrix64F grad1 = w2.scale(fdiff);
                        final Matrix64F grad2 = w1.scale(fdiff);

                        W.assignColumn(wordVecIdx, w1.sub(grad1.applyOnElements(gs1, (el1, el2) -> (el1 / (double) Math.sqrt(el2)))));
                        W.assignColumn(ctxVecIdx, w2.sub(grad2.applyOnElements(gs2, (el1, el2) -> (el1 / (double) Math.sqrt(el2)))));

                        B.set(0, wordVecIdx, (double) (b1 - fdiff / Math.sqrt(GradSqB.get(0, wordVecIdx))));
                        B.set(0, ctxVecIdx, (double) (b2 - fdiff / Math.sqrt(GradSqB.get(0, ctxVecIdx))));

                        gs1.assign(gs1.applyOnElements(grad1, (el1, el2) -> el1 + el2 * el2));
                        gs2.assign(gs2.applyOnElements(grad2, (el1, el2) -> el1 + el2 * el2));

                        GradSq.assignColumn(wordVecIdx, gs1);
                        GradSq.assignColumn(ctxVecIdx, gs2);

                        GradSqB.set(0, wordVecIdx, GradSqB.get(0, wordVecIdx) + fdiff * fdiff);
                        GradSqB.set(0, ctxVecIdx, GradSqB.get(0, ctxVecIdx) + fdiff * fdiff);
                    });
                });

                TransactionMng.commit(sync);
            })
        );
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.LOCAL
                .run(GloVe.class)
                .done();
    }
}