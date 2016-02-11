package de.tuberlin.pserver.examples.experiments.glove;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.commons.serialization.ObjectSerializer;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.metadata.DistScheme;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Random;


public final class GloveWithCompression extends Program {

    private static final long       ROWS = 20;

    private static final long       COLS = 36073; //3717689;

    private static final float      ALPHA = 0.75f;

    private static final int        X_MAX = 10;

    private static final float      LEARNING_RATE = 0.05f;

    private static final int        NUM_EPOCHS = 10;

    private static final String     INPUT_DATA = "datasets/text8_coocc.csv";

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = COLS, cols = COLS, path = INPUT_DATA)
    public SparseMatrix32F X;

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS * 2)
    public DenseMatrix32F W;

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS * 2)
    public DenseMatrix32F GradSq;

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = 1, cols = COLS * 2)
    public DenseMatrix32F B;

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = 1, cols = COLS * 2)
    public DenseMatrix32F GradSqB;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    private ObjectSerializer serializer = ObjectSerializer.Factory.create(ObjectSerializer.SerializerType.KRYO_SERIALIZER);

    private Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);

    /*@Transaction(state = "W, GradSq, B, GradSqB", mtxType = TransactionType.PULL)
    public final TransactionDefinition sync = new TransactionDefinition(

            (Prepare<Matrix32F, Pair<Integer, byte[]>>) (requestObjs, remoteMatrix) -> {
                final byte[] serializedObj = serializer.serialize(remoteMatrix);
                return Pair.of(serializedObj.length, compressor.compress(serializedObj));
            }
            ,

            (GenericApply<Pair<Integer, byte[]>,Matrix32F,Matrix32F>) (remoteUpdates, localState) -> {
                for (final Pair<Integer, byte[]> update : remoteUpdates) {
                    final Matrix32F updateMtx = serializer.deserialize(compressor.decompress(update.getRight(), update.getLeft()), DenseMatrix32F.class);
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
                        final Matrix32F w1 = W.getCol(wordVecIdx);
                        final float b1 = B.get(wordVecIdx);
                        final Matrix32F gs1 = GradSq.getCol(wordVecIdx);
                        final Matrix32F w2 = W.getCol(ctxVecIdx);
                        final float b2 = B.get(ctxVecIdx);
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

                //TransactionMng.commit(sync);
            })
        );
    }

    // ---------------------------------------------------
    // EntryImpl Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(GloveWithCompression.class)
                .done();

        //System.setProperty("pserver.profile", "wally");
        //PServerExecutor.REMOTE
        //        .run(GloVe.class)
        //        .done();
    }
}