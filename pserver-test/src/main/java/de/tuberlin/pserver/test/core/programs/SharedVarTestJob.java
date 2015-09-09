package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.SharedInt;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;


public class SharedVarTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            final SharedInt sharedInt = new SharedInt(slotContext, 0);

            CF.iterate().exe(1000, (e) -> sharedInt.inc());

            CF.syncSlots();

            CF.select().slot(0).exe(() -> Preconditions.checkState(sharedInt.get() == 4000));

            sharedInt.done();
        });
    }
}