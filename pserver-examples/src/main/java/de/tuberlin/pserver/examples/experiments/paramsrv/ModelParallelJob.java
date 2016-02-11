package de.tuberlin.pserver.examples.experiments.paramsrv;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.metadata.DistScheme;

public class ModelParallelJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, at = "0, 1", rows = 10, cols = 10)
    public Matrix32F parameters;

    @Matrix(scheme = DistScheme.REPLICATED, at = "2, 3", rows = 1, cols = 10)
    public Matrix32F cachedParameters;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(src = "parameters", dst = "cachedParameters", type = TransactionType.PULL)
    public final TransactionDefinition fetchParameters = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, parameterList, cachedParameters) -> {
                StringBuilder strBuilder = new StringBuilder();
                for (Matrix32F parameter : parameterList) {
                    strBuilder.append("PARTITION - ");
                    strBuilder.append(parameter.toString());
                    strBuilder.append("\n");
                }
                System.out.println(strBuilder.toString());
            }
    );

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit(at = "2")
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            TransactionMng.commit(fetchParameters);
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(ModelParallelJob.class)
                .done();
    }
}
