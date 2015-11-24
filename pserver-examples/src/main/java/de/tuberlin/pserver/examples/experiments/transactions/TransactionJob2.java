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
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;


public class TransactionJob2 extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.SINGLETON, cols = 100, rows = 100, at = "0")
    public Matrix32F globalModel;

    private final int[] paramIndices = new int[] { 12, 23, 67, 73, 87 };

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "globalModel", type = TransactionType.PULL)
    public final TransactionDefinition paramFetch = new TransactionDefinition(

        (Prepare<int[], float[]>) (paramIndices) -> {
            int i = 0;
            final float[] params = new float[paramIndices.length];
            for (int paramIdx : paramIndices)
                params[i++] = globalModel.get(0, paramIdx);
            return params;
        },

        (Apply<double[], double[]>) (params, state) -> params.get(0)
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit(at = "0")
    public void unit0(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            //UnitMng.loop(10, Loop.ASYNCHRONOUS, (e) -> {

            atomic(state(globalModel), () -> {
                for (int paramIdx : paramIndices) {
                    globalModel.set(0, paramIdx, 11.5f);
                }
            });

            Thread.sleep(10000);

            //});
        });
    }

    @Unit(at = "1")
    public void unit1(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            //UnitMng.loop(10, Loop.ASYNCHRONOUS, (e) -> {

            Thread.sleep(10);

            final double[] params = TransactionMng.commit(paramFetch, paramIndices);

            for (final double value : params)
                System.out.println("==> " + value);

        });
    }

    // ---------------------------------------------------
    // App Entry.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(TransactionJob2.class)
                .done();
    }
}
