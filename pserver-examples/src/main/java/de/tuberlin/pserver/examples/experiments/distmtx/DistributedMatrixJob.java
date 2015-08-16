package de.tuberlin.pserver.examples.experiments.distmtx;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.GlobalScope;
import de.tuberlin.pserver.dsl.state.State;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.types.PartitionType;

public class DistributedMatrixJob extends MLProgram {

    private static final int COLS = 20;

    @State(globalScope = GlobalScope.REPLICATED, rows = COLS, cols = COLS,
            path = "datasets/rowcolval_dataset.csv", format = Format.SPARSE_FORMAT)
    public Matrix X;

    @Override
    public void define(final Program program) {

        program.process(() -> {

            /*final Matrix.RowIterator iter = X.rowIterator();
            while (iter.hasNextRow()) {
                iter.nextRow();
                System.out.println(slotContext.programContext.runtimeContext.nodeID + " -> " + iter.getCurrentRowNum());
                X.set(iter.getCurrentRowNum(), 0, slotContext.programContext.runtimeContext.nodeID * 10);
            }

            double s = X.aggregateRows(Vector::sum).sum();
            System.out.println("s = " + s);*/
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(DistributedMatrixJob.class, 1)
                .done();
    }
}