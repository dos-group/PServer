package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.runtime.Program;

import java.util.Random;

public class GlobalSyncTestJob extends Program {

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            final Random rand = new Random();

            CF.loop().sync(Loop.GLOBAL).exe(10, (e) -> {

                Thread.sleep(rand.nextInt(1000));

                //System.out.println(slotContext + " -> " + e);
            });
        });
    }
}