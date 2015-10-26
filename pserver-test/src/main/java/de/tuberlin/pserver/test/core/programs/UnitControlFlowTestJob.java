package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;


public class UnitControlFlowTestJob extends Program {

    @Unit(at = "1")
    public void unit0(final Lifecycle lifecycle) {

        lifecycle.process( () -> System.out.println("Hello Unit 1.") );
    }

    @Unit(at = "2 - 5")
    public void unit1(final Lifecycle lifecycle) {

        lifecycle.process( () -> System.out.println("Hello Unit 2 - 5.") );
    }

    @Unit
    public void unit2(final Lifecycle lifecycle) {

        lifecycle.process( () -> System.out.println("Hello global Unit.") );
    }
}