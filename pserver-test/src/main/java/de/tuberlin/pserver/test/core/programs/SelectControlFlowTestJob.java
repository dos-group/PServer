package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;
import de.tuberlin.pserver.runtime.SlotGroup;

import java.util.Random;

public class SelectControlFlowTestJob extends MLProgram {

    @Unit
    public void main(final Program program) {

        program.process(() -> {

            CF.parUnit(0, 3).exe(() -> {

                final SlotGroup sg0 = slotContext.getActiveSlotGroup();

                System.out.println("LEVEL 1 " + sg0);

                Thread.sleep(new Random().nextInt(1000));

                CF.parUnit(1, 3).exe(() -> {

                    final SlotGroup sg1 = slotContext.getActiveSlotGroup();

                    System.out.println("LEVEL 2 " + sg1);

                    Thread.sleep(new Random().nextInt(1000));

                    CF.parUnit(2, 3).exe(() -> {

                        final SlotGroup sg2 = slotContext.getActiveSlotGroup();

                        System.out.println("LEVEL 3 " + sg2);

                        Thread.sleep(new Random().nextInt(1000));

                        CF.parUnit(3, 3).exe(() -> {

                            final SlotGroup sg3 = slotContext.getActiveSlotGroup();

                            System.out.println("LEVEL 4 " + sg3);

                            Thread.sleep(new Random().nextInt(1000));

                            try {

                                CF.parUnit(0, 3).exe(() -> {

                                    final SlotGroup sg4 = slotContext.getActiveSlotGroup();

                                    System.out.println("LEVEL 5 " + sg4);
                                });

                            } catch(Throwable e) {
                                System.out.println("SLOT ALLOCATION NOT POSSIBLE");
                            }
                        });
                    });
                });
            });

            CF.parUnit(0).exe(() -> System.out.println("SLOT 0"));
            CF.parUnit(1).exe(() -> System.out.println("SLOT 1"));
            CF.parUnit(2).exe(() -> System.out.println("SLOT 2"));
            CF.parUnit(3).exe(() -> System.out.println("SLOT 3"));
        });
    }
}