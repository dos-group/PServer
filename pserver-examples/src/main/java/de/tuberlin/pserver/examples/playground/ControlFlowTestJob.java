package de.tuberlin.pserver.examples.playground;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.runtime.JobExecutable;

public class ControlFlowTestJob extends JobExecutable {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

    }

    @Override
    public void compute() {

        CF.select().instance(0, 5).exe(() -> {

            CF.select().instance(0,3).exe(() -> {

                //CF.select().instance(4, 7).exe(() -> System.out.println("!"));

                final int dop = slotContext.jobContext.executionManager.getDegreeOfParallelism();
                CF.select().instance(0).exe(() -> System.out.println("DOP = " + dop));
            });

        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(ControlFlowTestJob.class, 8)
                .done();
    }
}

