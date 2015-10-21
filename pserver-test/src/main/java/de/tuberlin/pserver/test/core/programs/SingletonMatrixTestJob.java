package de.tuberlin.pserver.test.core.programs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix64F;

public class SingletonMatrixTestJob extends Program {

    private static final int ROWS = 100;

    private static final int COLS = 100;

    @State(scope = Scope.SINGLETON, at = "0", rows = ROWS, cols = COLS)
    public Matrix64F W;

    @Unit(at = "1 - 3")
    public void main(final Lifecycle lifecycle) {
        lifecycle.process(() -> {

            final int rows = (ROWS / (programContext.runtimeContext.numOfNodes));

            for (int i = programContext.nodeID * rows; i < programContext.nodeID * rows + rows; ++i) {
                for (int j = 0; j < COLS; ++j) {

                    W.set(i, j, (double)programContext.nodeID);
                    final double value = W.get(i, j);

                    Preconditions.checkState(value == programContext.nodeID,
                            value + " != " + programContext.nodeID + " - " + programContext);
                }
            }
        });
    }
}