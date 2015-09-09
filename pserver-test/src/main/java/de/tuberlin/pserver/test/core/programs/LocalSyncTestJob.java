package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.dsl.controlflow.iteration.Iteration;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;

import java.util.Random;


public class LocalSyncTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            CF.select().node(0).slot(1, 3).exe(() -> {

                final Random rand = new Random();

                CF.iterate().sync(Iteration.LOCAL).exe(10, (e) -> {

                    Thread.sleep(rand.nextInt(100));

                    //System.out.println(slotContext + " -> " + e);
                });
            });
        });
    }
}
