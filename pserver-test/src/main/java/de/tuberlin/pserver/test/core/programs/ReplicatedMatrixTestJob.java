package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public class ReplicatedMatrixTestJob extends Program {

    @Load(filePath = "datasets/rowcolval_dataset.csv")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = 21, cols = 20)
    public Matrix32F X;

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            if (programContext.node(3)) {

                final Matrix32F.RowIterator it = X.rowIterator();
                while (it.hasNext()) {
                    it.next();
                    //System.out.println(it.value(0));
                }
            }
        });
    }
}