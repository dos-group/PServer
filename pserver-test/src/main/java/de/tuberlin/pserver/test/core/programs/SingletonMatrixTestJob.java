package de.tuberlin.pserver.test.core.programs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public class SingletonMatrixTestJob extends Program {

    private static final int ROWS = 100;

    private static final int COLS = 100;

    @Matrix(scheme = DistScheme.SINGLETON, at = "0", rows = ROWS, cols = COLS)
    public Matrix32F W;

    @Unit(at = "1 - 3")
    public void main(final Lifecycle lifecycle) {
        lifecycle.process(() -> {

            final int rows = (ROWS / (programContext.runtimeContext.numOfNodes));

            for (int i = programContext.nodeID * rows; i < programContext.nodeID * rows + rows; ++i) {
                for (int j = 0; j < COLS; ++j) {

                    W.set(i, j, programContext.nodeID);
                    final double value = W.get(i, j);

                    Preconditions.checkState(value == programContext.nodeID,
                            value + " != " + programContext.nodeID + " - " + programContext);
                }
            }
        });
    }
}