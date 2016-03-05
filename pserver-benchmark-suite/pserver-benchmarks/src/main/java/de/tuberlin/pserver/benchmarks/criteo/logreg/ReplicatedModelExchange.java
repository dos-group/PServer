package de.tuberlin.pserver.benchmarks.criteo.logreg;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.core.config.ConfigLoader;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.util.Random;

public final class ReplicatedModelExchange extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_EPOCHS = 15;

    private static final long COLS = 1048615;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F W;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(
            (Update<Matrix32F>) (requestObj, r, l) -> {
                for (final Matrix32F update : r)
                    Parallel.For(update, (i, j, v) -> W.set(i, j, W.get(i, j) + update.get(i, j)));
                W.scale(1.0f / 2, W);
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            Random rand = new Random();
            for (int i = 0; i < NUM_EPOCHS; ++i) {
                atomic(state(W), () -> {
                    for (int j = 0; j < COLS; ++j) {
                        float val = rand.nextFloat();
                        W.set(0, j, val);
                    }
                });
                TransactionMng.commit(syncW);
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), ReplicatedModelExchange.class)
                .done();
    }
}
