package de.tuberlin.pserver.examples.experiments.paramsrv;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Combine;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;


public class DataParallelJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = 10)
    public Matrix32F model;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(state = "model", type = TransactionType.PUSH)
    public final TransactionDefinition syncModel = new TransactionDefinition(

            (Combine<Matrix32F>) (requestObj, gradients) -> {
                Matrix32F combinedGradient = gradients.get(0);
                for (int i = 1; i < gradients.size(); ++i) {
                    combinedGradient.add(gradients.get(i), combinedGradient);
                }
                return combinedGradient;
            },

            (Update<Matrix32F>) (requestObj, remoteModels, localModel) -> {
                StringBuilder strBuilder = new StringBuilder();
                for (Matrix32F remoteModel : remoteModels) {
                    for (int i = 0; i < 10; ++i)
                        strBuilder.append("[" + programContext.nodeID + "] -> " + remoteModel.get(i) + "\n");
                }
                System.out.println(strBuilder.toString());
            }
    );

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit()
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            for (int i = 0; i < 10; ++i)
                model.set(0, i, (float) i * programContext.nodeID);

            TransactionMng.commit(syncModel);
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(DataParallelJob.class)
                .done();
    }
}
