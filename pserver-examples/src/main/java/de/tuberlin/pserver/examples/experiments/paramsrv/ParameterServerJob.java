package de.tuberlin.pserver.examples.experiments.paramsrv;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Combine;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;

public class ParameterServerJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(at = "0", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 1, cols = 10)
    public Matrix32F parameters;

    @State(at = "1 - 3", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 1, cols = 10)
    public Matrix32F gradients;

    //@State(scope = Scope.REPLICATED, matrixFormat = Format.DENSE_FORMAT, rows = 1, cols = 10)
    //public Matrix32F model;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(src = "gradients", dst = "parameters", type = TransactionType.PUSH)
    public final TransactionDefinition updateParameters = new TransactionDefinition(

            (Combine<Matrix32F>) (gradients) -> {
                Matrix32F combinedGradient = gradients.get(0);
                for (int i = 1; i < gradients.size(); ++i) {
                    combinedGradient.add(gradients.get(i), combinedGradient);
                }
                return combinedGradient;
            },

            (Update<Matrix32F>) (gradients, parameters) -> {
                for (Matrix32F gradient : gradients) {
                    for (int i = 0; i < 10; ++i)
                        System.out.println(gradient.get(i));
                }
            }
    );

    /*@Transaction(state = "model", type = TransactionType.PUSH)
    public final TransactionDefinition syncModel = new TransactionDefinition(

            (Update<Matrix32F>) (remoteModels, localModel) -> {
                for (Matrix32F remoteModel : remoteModels) {
                    for (int i = 0; i < 10; ++i)
                        System.out.println(remoteModel.get(i));
                }
            }
    );*/

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit(at = "1 - 3")
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            for (int i = 0; i < 10; ++i)
                gradients.set(0, i, (float) i * programContext.nodeID);

            TransactionMng.commit(updateParameters);

            /*for (int i = 0; i < 10; ++i)
                model.set(0, i, (float) i * programContext.nodeID);

            TransactionMng.commit(syncModel);*/
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(ParameterServerJob.class)
                .done();
    }
}
