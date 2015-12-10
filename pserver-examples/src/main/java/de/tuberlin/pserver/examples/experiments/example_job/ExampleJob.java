package de.tuberlin.pserver.examples.experiments.example_job;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Matrix32F;

public class ExampleJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.REPLICATED, rows = 100, cols = 100, format = Format.SPARSE_FORMAT)
    public Matrix32F W;

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.process(() -> {




            //mtxWriter.write("model.csv", DenseMatrixFormat.class, W);
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        System.setProperty("simulation.numNodes", "1");

        PServerExecutor.LOCAL
                .run(ExampleJob.class)
                .done();
    }
}