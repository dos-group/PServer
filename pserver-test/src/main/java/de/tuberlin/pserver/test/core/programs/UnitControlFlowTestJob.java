package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;


public class UnitControlFlowTestJob extends MLProgram {

    @Unit(at = "1")
    public void main0(final Program program) {

        program.process( () -> System.out.println("Hello Unit 1.") );
    }

    @Unit
    public void main1(final Program program) {

        program.process( () -> System.out.println("Hello global Unit.") );
    }
}