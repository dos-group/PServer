package de.tuberlin.pserver.examples.experiments.libsvm;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.types.matrix.f32.Matrix32F;
import de.tuberlin.pserver.types.matrix.f32.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.f32.sparse.CSRMatrix32F;

public class LibSVMReaderJob extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int NUM_SIMULATION_NODES = 2;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED, rows = 16000, cols = 1)
    public DenseMatrix32F XTrainLabel;

    @State(scope = Scope.PARTITIONED, rows = 270, cols = 13, path = "datasets/svmSmallTestFile",
            fileFormat = FileFormat.SVM_FORMAT, labels = "XTrainLabel")

    public CSRMatrix32F XTrainFeatures;

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(final Lifecycle lifecycle) {

        lifecycle.process(() -> {
            Thread.sleep(2000 * programContext.nodeID);
            int i = 0;
            Matrix32F.RowIterator it = XTrainFeatures.rowIterator();
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
                .run(LibSVMReaderJob.class)
                .done();
    }
}