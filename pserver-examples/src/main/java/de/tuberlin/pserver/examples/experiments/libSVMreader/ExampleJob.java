package de.tuberlin.pserver.examples.experiments.libSVMreader;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;

public class ExampleJob extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_SIMULATION_NODES = 2;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED, rows = 270, cols = 13, matrixFormat = MatrixFormat.DENSE_FORMAT,
            path = "datasets/svmSmallTestFile", fileFormat = FileFormat.SVM_FORMAT)
    public Matrix32F XTrain;

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            Thread.sleep(2000 * programContext.nodeID);

            int i = 0;
            Matrix32F.RowIterator it = XTrain.rowIterator();
            while (it.hasNext()) {
                it.next();
                System.out.println("entry " + (i++) + " at node [" + programContext.nodeID + "] data - " + it.get());
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