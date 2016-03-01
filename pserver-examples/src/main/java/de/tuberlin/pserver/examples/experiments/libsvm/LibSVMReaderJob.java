package de.tuberlin.pserver.examples.experiments.libsvm;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public class LibSVMReaderJob extends Program {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String FILE_PATH = "datasets/svmSmallTestFile";

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = 16000, cols = 1)
    public DenseMatrix32F XTrainLabel;

    @Load(filePath = FILE_PATH, labels = "XTrainLabel")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = 270, cols = 13)
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
        System.setProperty("global.simNodes", "2");
        PServerExecutor.LOCAL
                .run(LibSVMReaderJob.class)
                .done();
    }
}