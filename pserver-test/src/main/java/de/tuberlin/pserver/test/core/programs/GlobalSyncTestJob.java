package de.tuberlin.pserver.test.core.programs;


import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;

import java.util.Random;

public class GlobalSyncTestJob extends Program {

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            final Random rand = new Random();

            UnitMng.loop(10, Loop.BULK_SYNCHRONOUS, (e) -> {

                Thread.sleep(rand.nextInt(1000));

                //System.out.println(programContext + " -> " + e);
            });
        });
    }
}