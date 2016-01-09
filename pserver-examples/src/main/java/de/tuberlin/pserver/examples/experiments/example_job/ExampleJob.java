package de.tuberlin.pserver.examples.experiments.example_job;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;

public class ExampleJob extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_SIMULATION_NODES = 3;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    // Matrix will be partitioned by row over the nodes.
    @State(scope = Scope.PARTITIONED, rows = 4, cols = 3, format = FileFormat.DENSE_FORMAT, path = "datasets/X_train.csv")
    public Matrix32F XTrain;

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.process(() -> {
            Matrix32F.RowIterator it = XTrain.rowIterator();
            while (it.hasNext()) {
                it.next();
                System.out.println("at node [" + programContext.nodeID + "] data - " + it.get());
            }
        });
    }

    // ---------------------------------------------------
    // EntryImpl Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        // Configure the number of simulation nodes.
        System.setProperty("simulation.numNodes", String.valueOf(NUM_SIMULATION_NODES));

        PServerExecutor.LOCAL
                .run(ExampleJob.class)
                .done();
    }
}