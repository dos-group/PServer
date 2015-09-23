package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.MLProgram;

public class SingletonMatrixTestJob extends MLProgram {

    private static final int ROWS = 1000;

    private static final int COLS = 1000;

    @State(globalScope = GlobalScope.SINGLETON, at = "0", rows = ROWS, cols = COLS)
    public Matrix W;

    @Unit(at = "1 - 3")
    public void main(final Program program) {
        /*program.process(() -> {

            final int rows = ((ROWS / (slotContext.programContext.nodeDOP - 1)) / slotContext.programContext.perNodeDOP);

            for (int k = 0; k < slotContext.programContext.perNodeDOP; ++k) {

                CF.parUnit(k).exe(() -> {

                    for (int i = slotContext.slotID * rows; i < slotContext.slotID * rows + rows; ++i) {
                        for (int j = 0; j < COLS; ++j) {

                            W.set(i, j, slotContext.slotID);
                            final double value = W.get(i, j);

                            Preconditions.checkState(value == slotContext.slotID,
                                    value + " != " + slotContext.slotID + " - " + slotContext);
                        }
                    }
                });
            }
        });*/
    }
}