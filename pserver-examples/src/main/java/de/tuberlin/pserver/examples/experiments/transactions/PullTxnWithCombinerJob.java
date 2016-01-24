package de.tuberlin.pserver.examples.experiments.transactions;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Combine;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;

public class PullTxnWithCombinerJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 1, cols = 10)
    public Matrix32F model;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(state = "model", type = TransactionType.PULL)
    public final TransactionDefinition syncModel = new TransactionDefinition(

            (Combine<Matrix32F>) (models) -> {
                Matrix32F combinedModel = models.get(0);
                for (int i = 1; i < models.size(); ++i) {
                    combinedModel.add(models.get(i), combinedModel);
                }
                return combinedModel;
            },

            (Update<Matrix32F>) (remoteModels, localModel) -> {
                StringBuilder strBuilder = new StringBuilder();
                for (Matrix32F remoteModel : remoteModels) {
                    for (int i = 0; i < 10; ++i)
                        strBuilder.append("[")
                                .append(programContext.nodeID)
                                .append("] -> ")
                                .append(remoteModel.get(i))
                                .append("\n");
                }

                if (programContext.node(0))
                    System.out.println(strBuilder.toString());
            }
    );

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit()
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            model.set(0, programContext.nodeID, (float)(programContext.nodeID + 1));

            if (programContext.node(0))
                TransactionMng.commit(syncModel);
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(PullTxnWithCombinerJob.class)
                .done();
    }

}
