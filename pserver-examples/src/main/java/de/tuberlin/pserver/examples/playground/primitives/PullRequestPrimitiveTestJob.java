package de.tuberlin.pserver.examples.playground.primitives;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.runtime.JobExecutable;

import java.util.Random;

public class PullRequestPrimitiveTestJob extends JobExecutable {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.registerPullRequestHandler("pull-request-value", name -> new Random().nextDouble());
    }

    @Override
    public void compute() {

        while (true) {

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (slotContext.jobContext.nodeID == 0) {

                final Object[] results = dataManager.pullRequest("pull-request-value");

                for (final Object o : results)
                    LOG.info(o == null ? "null" : o.toString());

                LOG.info("------------");
            }
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(PullRequestPrimitiveTestJob.class)
                .done();
    }
}
