package de.tuberlin.pserver.examples.experiments.transactions;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Matrix32F;

import java.util.Random;

public class TransactionJob1 extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.REPLICATED, cols = 100, rows = 100)
    public Matrix32F model;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "model", type = TransactionType.PULL)
    public TransactionDefinition modelSync = new TransactionDefinition(

        (Prepare<Matrix32F, Matrix32F>) model -> model,

        (Apply<Matrix32F, Void>) (remoteModels, localModel) -> {

            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 100; j++) {
                    float val = 0f;
                    for (final Matrix32F remoteModel : remoteModels)
                        val += remoteModel.get(i, j);
                    val += model.get(i, j);
                    model.set(i, j, (model.get(i, j) + val) / 4);
                }
            }
            return null;
        }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            UnitMng.loop(10, Loop.ASYNCHRONOUS, (e) -> {

                atomic(state(model), () -> {

                    final Random rand = new Random();
                    for (int i = 0; i < 100; i++) {
                        for (int j = 0; j < 100; j++) {
                            model.set(i, j, (float)rand.nextDouble());
                        }
                    }

                });

                TransactionMng.commit(modelSync);
            });
        });
    }

    // ---------------------------------------------------
    // App Entry.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(TransactionJob1.class)
                .done();
    }
}
