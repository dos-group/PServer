package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.mcruntime.shared.SharedInt;
import de.tuberlin.pserver.runtime.Program;


public class SharedVarTestJob extends Program {

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            final SharedInt sharedInt = new SharedInt(programContext, 0);

            UnitMng.loop(1000, (e) -> sharedInt.inc());

            Preconditions.checkState(sharedInt.get() == 1000);

            sharedInt.done();
        });
    }
}