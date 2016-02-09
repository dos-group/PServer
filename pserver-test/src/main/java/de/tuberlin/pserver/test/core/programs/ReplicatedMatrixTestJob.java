package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionerType;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix64F;

public class ReplicatedMatrixTestJob extends Program {

    @State(scope = Scope.PARTITIONED,
            rows = 21, cols = 20,
            path = "datasets/rowcolval_dataset.csv",
            partitioner = PartitionerType.ROW_PARTITIONER)

    public Matrix64F X;

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            if (programContext.node(3)) {

                final Matrix64F.RowIterator it = X.rowIterator();
                while (it.hasNext()) {
                    it.next();
                    //System.out.println(it.value(0));
                }
            }
        });
    }
}