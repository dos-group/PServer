package de.tuberlin.pserver.examples.experiments.distmtx;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Matrix;

public class DistributedMatrixJob extends Program {

    @State(scope = Scope.REPLICATED, rows = 100, cols = 100)
    public Matrix model;

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            UnitMng.loop(100, Loop.BULK_SYNCHRONOUS, (e) -> {

                System.out.println(e);
            });
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        System.setProperty("simulation.numNodes", "4");

        PServerExecutor.LOCAL
                .run(DistributedMatrixJob.class)
                .done();
    }
}