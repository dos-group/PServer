package de.tuberlin.pserver.examples.experiments.regression;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.util.Random;


public class LogRegCriteoRM extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int COLS = 1048615 * 2;
    private static int NUM_EPOCHS = 15;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition syncW = new TransactionDefinition(

        (Update<Matrix32F>) (requestObj, remoteUpdates, localState) -> {
            int count = 1;
            if (remoteUpdates != null) {
                for (final Matrix32F update : remoteUpdates) {
                    Parallel.For(update, (i, j, v) -> W.set(i, j, W.get(i, j) + update.get(i, j)));
                    count++;
                }
                W.scale(1.0f / count, W);
            }
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
                /*atomic(state(W), () -> {
                    for (int j = 0; j < COLS; ++j) {
                        W.set(0, j, rand.nextFloat());
                    }
                });
                Thread.sleep(10000);*/
                TransactionMng.commit(syncW);
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("global.simNodes", "4");

        PServerExecutor.LOCAL
                .run(LogRegCriteoRM.class)
                .done();
    }
}
