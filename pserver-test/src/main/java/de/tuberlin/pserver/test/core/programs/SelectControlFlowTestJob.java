package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.SlotGroup;

import java.util.Random;

public class SelectControlFlowTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            CF.select().slot(0, 3).exe(() -> {

                final SlotGroup sg0 = slotContext.programContext.runtimeContext.executionManager.getActiveSlotGroup();

                //System.out.println("LEVEL 1 " + sg0);

                Thread.sleep(new Random().nextInt(1000));

                CF.select().slot(1, 3).exe(() -> {

                    final SlotGroup sg1 = slotContext.programContext.runtimeContext.executionManager.getActiveSlotGroup();

                    //System.out.println("LEVEL 2 " + sg1);

                    Thread.sleep(new Random().nextInt(1000));

                    CF.select().slot(2, 3).exe(() -> {

                        final SlotGroup sg2 = slotContext.programContext.runtimeContext.executionManager.getActiveSlotGroup();

                        //System.out.println("LEVEL 3 " + sg2);

                        Thread.sleep(new Random().nextInt(1000));

                        CF.select().slot(3, 3).exe(() -> {

                            final SlotGroup sg3 = slotContext.programContext.runtimeContext.executionManager.getActiveSlotGroup();

                            //System.out.println("LEVEL 4 " + sg3);

                            Thread.sleep(new Random().nextInt(1000));

                            try {
                                CF.select().slot(0, 3).exe(() -> {

                                    final SlotGroup sg4 = slotContext.programContext.runtimeContext.executionManager.getActiveSlotGroup();

                                    System.out.println("LEVEL 5 " + sg4);
                                });

                            } catch(Throwable e) {
                                System.out.println("SLOT ALLOCATION NOT POSSIBLE");
                            }
                        });
                    });
                });
            });
        });
    }
}