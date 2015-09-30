package de.tuberlin.pserver.examples.experiments;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.TransactionType;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Apply;
import de.tuberlin.pserver.dsl.transaction.phases.Prepare;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.util.List;
import java.util.Random;

public class TransactionJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(globalScope = GlobalScope.REPLICATED, cols = 100, rows = 100)
    public Matrix model;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "model", type = TransactionType.PULL_WRITE)
    public TransactionDefinition<List<Matrix>> modelSync = new TransactionDefinition<>(

            new Prepare<List<Matrix>>() {
                @Override
                public List<Matrix> prepare(List<Matrix> object) throws Exception {



                    return object;
                }
            },

            new Apply<List<Matrix>>() {

                @Override
                public List<Matrix> apply(final List<Matrix> remoteModels) throws Exception {
                    for (int i = 0; i < 100; i++) {
                        for (int j = 0; j < 100; j++) {
                            double val = 0.0;
                            for (final Matrix remoteModel : remoteModels)
                                val += remoteModel.get(i, j);
                            val += model.get(i, j);
                            model.set(i, j, (model.get(i, j) + val) / 4);
                        }
                    }
                    return null;
                }
            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.process(() ->

            UnitMng.loop(10, Loop.ASYNC, (e) -> {

                atomic(state(model), () -> {

                    final Random rand = new Random();
                    for (int i = 0; i < 100; i++) {
                        for (int j = 0; j < 100; j++) {
                            model.set(i, j, rand.nextDouble());
                        }
                    }

                });

                TransactionMng.commit(modelSync);
            })
        );
    }

    // ---------------------------------------------------
    // App Entry.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(TransactionJob.class)
                .done();
    }
}
