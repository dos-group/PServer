package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.dsl.controlflow.iteration.Loop;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

import java.util.Random;

public class GlobalSyncTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            final Random rand = new Random();

            CF.loop().sync(Loop.GLOBAL).exe(10, (e) -> {

                Thread.sleep(rand.nextInt(1000));

                //System.out.println(slotContext + " -> " + e);
            });
        });
    }
}