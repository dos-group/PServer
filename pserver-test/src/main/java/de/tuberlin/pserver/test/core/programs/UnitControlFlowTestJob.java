package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.runtime.Program;


public class UnitControlFlowTestJob extends Program {

    @Unit(at = "1")
    public void main0(final Lifecycle lifecycle) {

        lifecycle.process( () -> System.out.println("Hello Unit 1.") );
    }

    @Unit
    public void main1(final Lifecycle lifecycle) {

        lifecycle.process( () -> System.out.println("Hello global Unit.") );
    }
}