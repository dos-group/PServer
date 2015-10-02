package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix;

public class SingletonMatrixTestJob extends Program {

    private static final int ROWS = 1000;

    private static final int COLS = 1000;

    @State(globalScope = GlobalScope.SINGLETON, at = "0", rows = ROWS, cols = COLS)
    public Matrix W;

    @Unit(at = "1 - 3")
    public void main(final Lifecycle lifecycle) {
        /*program.process(() -> {

            final int rows = ((ROWS / (programContext.programContext.nodeDOP - 1)) / programContext.programContext.perNodeDOP);

            for (int k = 0; k < programContext.programContext.perNodeDOP; ++k) {

                CF.parUnit(k).exe(() -> {

                    for (int i = programContext.id * rows; i < programContext.id * rows + rows; ++i) {
                        for (int j = 0; j < COLS; ++j) {

                            W.set(i, j, programContext.id);
                            final double value = W.get(i, j);

                            Preconditions.checkState(value == programContext.id,
                                    value + " != " + programContext.id + " - " + programContext);
                        }
                    }
                });
            }
        });*/
    }
}