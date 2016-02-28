package de.tuberlin.pserver.examples.experiments.glove;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;
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
    // Large Experiment

    /*private static final long       ROWS = 20;

    private static final long       COLS = 3717689;

    private static final float      ALPHA = 0.75f;

    private static final int        X_MAX = 10;

    private static final float      LEARNING_RATE = 0.05f;

    private static final int        NUM_EPOCHS = 10;

    private static final String     INPUT_DATA = "/input/reddit/cooccmat_mincount_15_windowsize_15"*/


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

    @Load(filePath = INPUT_DATA)
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = COLS, cols = COLS)
    public Matrix32F X;

    @Matrix(scheme = DistScheme.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix32F W;

    @Matrix(scheme = DistScheme.REPLICATED, rows = ROWS, cols = COLS * 2)
    public Matrix32F GradSq;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS * 2)
    public Matrix32F B;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS * 2)
    public Matrix32F GradSqB;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W, GradSq, B, GradSqB", type = TransactionType.PULL)
    public final TransactionDefinition sync = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, remoteUpdates, localState) -> {
                for (final Matrix32F update : remoteUpdates)
                    Parallel.For(update, (i, j, v) -> localState.set(i, j, (localState.get(i, j) + update.get(i, j)) / 2));
            }
    );

    /*private ObjectSerializer serializer = ObjectSerializer.Factory.create(ObjectSerializer.SerializerType.KRYO_SERIALIZER);
    private Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);
    @Transaction(state = "W, GradSq, B, GradSqB", mtxType = TransactionType.PUSH)
    public final TransactionDefinition sync = new TransactionDefinition(
            (Prepare<Matrix32, Pair<Integer, byte[]>>) (remoteMatrix) -> {
                final byte[] serializedObj = serializer.serialize(remoteMatrix);
                return Pair.of(serializedObj.length, compressor.compress(serializedObj));
            },
            (GenericApply<Pair<Integer, byte[]>,Matrix32,Matrix32>) (remoteUpdates, localState) -> {
                for (final Pair<Integer, byte[]> update : remoteUpdates) {
                    final Matrix32 updateMtx = serializer.deserialize(compressor.decompress(update.getRight(), update.getLeft()), DenseMatrix32.class);
                    Parallel.For(updateMtx, (i, j, v) -> localState.set(i, j, (localState.get(i, j) + updateMtx.get(i, j)) / 2));
                }
                return null;
            }
    );*/

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

            final Random rand = new Random();
            W.applyOnElements(v -> (float)(rand.nextDouble() - 0.5) / ROWS);
            B.applyOnElements(v -> (float)(rand.nextDouble() - 0.5) / ROWS);
            GradSq.assign(1.0f);
            GradSqB.assign(1.0f);

        }).process(() ->

            UnitMng.loop(NUM_EPOCHS, (e0) -> {

                final MutableDouble costI = new MutableDouble(0.0);

                LOG.info("Epoch = " + e0 + " at Node " + programContext.nodeID);

                atomic(state(W, B, GradSq, GradSqB), () -> {
                    Parallel.For(X, (wordVecIdx, j, v) -> {
                        if (v == 0) return;

                        final long ctxVecIdx = j + COLS;
                        final Matrix32F w1  = W.getCol(wordVecIdx);
                        final float     b1  = B.get(wordVecIdx);
                        final Matrix32F gs1 = GradSq.getCol(wordVecIdx);
                        final Matrix32F w2  = W.getCol(ctxVecIdx);
                        final float     b2  = B.get(ctxVecIdx);
                        final Matrix32F gs2 = GradSq.getCol(ctxVecIdx);

                        final float diff = w1.dot(w2) + b1 + b2 - (float) Math.log(v);
                        float fdiff = (v > X_MAX) ? diff : (float) Math.pow(v / X_MAX, ALPHA) * diff;

                        costI.add(0.5 * diff * fdiff);

                        fdiff *= LEARNING_RATE;

                        final Matrix32F grad1 = w2.scale(fdiff);
                        final Matrix32F grad2 = w1.scale(fdiff);

                        W.assignColumn(wordVecIdx, w1.sub(grad1.applyOnElements(gs1, (el1, el2) -> (el1 / (float) Math.sqrt(el2)))));
                        W.assignColumn(ctxVecIdx, w2.sub(grad2.applyOnElements(gs2, (el1, el2) -> (el1 / (float) Math.sqrt(el2)))));

                        B.set(0, wordVecIdx, (float) (b1 - fdiff / Math.sqrt(GradSqB.get(0, wordVecIdx))));
                        B.set(0, ctxVecIdx, (float) (b2 - fdiff / Math.sqrt(GradSqB.get(0, ctxVecIdx))));

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

        System.setProperty("global.simNodes", "2");

        PServerExecutor.LOCAL
                .run(GloVe.class)
                .done();

        //System.setProperty("pserver.profile", "wally");
        //PServerExecutor.REMOTE
        //        .run(GloVe.class)
        //        .done();
    }
}