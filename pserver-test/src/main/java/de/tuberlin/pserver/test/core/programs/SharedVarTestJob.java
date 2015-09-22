package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedInt;
import de.tuberlin.pserver.runtime.MLProgram;


public class SharedVarTestJob extends MLProgram {

    @Unit
    public void main(final Program program) {

        program.process(() -> {

            final SharedInt sharedInt = new SharedInt(slotContext, 0);

            CF.loop().exe(1000, (e) -> sharedInt.inc());

            CF.syncSlots();

            CF.serial().exe(() -> Preconditions.checkState(sharedInt.get() == 4000));

            sharedInt.done();
        });
    }
}