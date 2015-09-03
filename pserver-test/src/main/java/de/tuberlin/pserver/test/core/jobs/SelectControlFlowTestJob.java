package de.tuberlin.pserver.test.core.jobs;


import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

public class SelectControlFlowTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            CF.select().slot(0, 2).exe(() -> {

                //final Pair<Integer, Integer> sr0 = slotContext.programContext.runtimeContext.executionManager.getAvailableSlotRangeForScope();

                //System.out.println("[" + sr0.getLeft() + ", " + sr0.getRight() + "]");

                CF.select().slot(0, 1).exe(() -> {

                    //final Pair<Integer, Integer> sr1 = slotContext.programContext.runtimeContext.executionManager.getAvailableSlotRangeForScope();

                    //System.out.println("[" + sr1.getLeft() + ", " + sr1.getRight() + "]");
                });
            });
        });
    }
}