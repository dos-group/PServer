package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedInt;
import de.tuberlin.pserver.runtime.Program;


public class SharedVarTestJob extends Program {

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            final SharedInt sharedInt = new SharedInt(slotContext, 0);

            CF.loop().exe(1000, (e) -> sharedInt.inc());

            Preconditions.checkState(sharedInt.get() == 1000);

            sharedInt.done();
        });
    }
}