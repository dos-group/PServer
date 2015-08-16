package de.tuberlin.pserver.examples.experiments.distmtx;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.GlobalScope;
import de.tuberlin.pserver.dsl.state.State;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;

public class DistributedMatrixJob extends MLProgram {

    @State(globalScope = GlobalScope.LOGICALLY_PARTITIONED, partitionType = PartitionType.ROW_PARTITIONED,
            rows = 20, cols = 20, format = Format.DENSE_FORMAT)
    public DistributedMatrix X;

    @Override
    public void define(final Program program) {

        program.process(() -> {

            CF.iterate().exe(X, (i, iter) -> {
                X.set(iter.getCurrentRowNum(), 0, slotContext.programContext.runtimeContext.nodeID + 1);
            });

            X.syncPartitions();

            CF.select().node(1).exe(() -> {
                for (int i = 0; i < X.numRows(); ++i)
                    System.out.println(X.get(i, 0));
            });

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