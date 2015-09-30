package de.tuberlin.pserver.examples.experiments.distmtx;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;

public class DistributedMatrixJob extends Program {

    @State(globalScope = GlobalScope.LOGICALLY_PARTITIONED, partitionType = PartitionType.ROW_PARTITIONED,
            rows = 20, cols = 20, format = Format.DENSE_FORMAT)
    public DistributedMatrix X;

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            /*CF.loop().exe(X, (i, iter) -> {
                X.set(iter.rowNum(), 0, programContext.programContext.runtimeContext.nodeID + 1);
            });

            X.collectRemotePartitions();

            CF.parUnit().node(1).exe(() -> {
                for (int i = 0; i < X.rows(); ++i)
                    System.out.println(X.get(i, 0));
            });*/

            /*final Matrix.RowIterator iter = X.rowIterator();
            while (iter.hasNext()) {
                iter.next();
                System.out.println(programContext.programContext.runtimeContext.nodeID + " -> " + iter.rowNum());
                X.set(iter.rowNum(), 0, programContext.programContext.runtimeContext.nodeID * 10);
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
                .run(DistributedMatrixJob.class)
                .done();
    }
}