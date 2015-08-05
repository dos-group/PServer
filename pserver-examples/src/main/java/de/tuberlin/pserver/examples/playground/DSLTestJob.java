package de.tuberlin.pserver.examples.playground;


import de.tuberlin.pserver.app.PServerJob;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.cf.ControlFlow;
import de.tuberlin.pserver.dsl.df.DataFlow;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;

public class DSLTestJob {

    // ---------------------------------------------------
    // Job.
    // ---------------------------------------------------

    public class APIDesignJob extends PServerJob {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private Matrix X;

        private final ControlFlow CF;

        private final DataFlow DF;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public APIDesignJob() {

            this.CF = new ControlFlow(instanceContext);

            this.DF = new DataFlow(instanceContext);
        }

        // ---------------------------------------------------
        // Life-Cycle.
        // ---------------------------------------------------

        @Override
        public void prologue() {

            final Matrix X = new MatrixBuilder()
                    .dimension(1000, 1000)
                    .format(Matrix.Format.DENSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .build();

            DF.put("X", X);
        }

        @Override
        public void compute() {

            X = DF.get("X");

            CF.iterate()
                    .async()
                    .execute(15, () -> {

                        CF.select()
                                .allNodes()
                                .instance(0)
                                .execute(() -> {

                                    CF.iterate()
                                            .execute(X, (iter) -> {

                                            });
                                });
                    });
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        PServerExecutor.LOCAL
                .run(APIDesignJob.class, 4)
                .done();
    }
}