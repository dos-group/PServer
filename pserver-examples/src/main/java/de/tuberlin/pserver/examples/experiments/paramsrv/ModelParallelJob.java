package de.tuberlin.pserver.examples.experiments.paramsrv;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;

public class ModelParallelJob extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(at = "0", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 1, cols = 10)
    public Matrix32F parameters;

    @State(at = "1 - 3", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 1, cols = 10)
    public Matrix32F gradients;

    @State(at = "1 - 3", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 1, cols = 10)
    public Matrix32F cachedParameters;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(src = "gradients", dst = "parameters", type = TransactionType.PUSH)
    public final TransactionDefinition updateParameters = new TransactionDefinition(

            /*(Combine<Matrix32F>) (gradients) -> {
                Matrix32F combinedGradient = gradients.get(0);
                for (int i = 1; i < gradients.size(); ++i) {
                    combinedGradient.add(gradients.get(i), combinedGradient);
                }
                return combinedGradient;
            },*/

            (Update<Matrix32F>) (gradients, parameters) -> {
                for (Matrix32F gradient : gradients)
                    parameters.add(gradient, parameters);

                System.out.println("Parameters at [" + programContext.nodeID + "] => " + parameters);
            }
    );

    @Transaction(src = "parameters", dst = "cachedParameters", type = TransactionType.PULL)
    public final TransactionDefinition fetchParameters = new TransactionDefinition(

            (Update<Matrix32F>) (parameterList, cachedParameters) -> {
                for (Matrix32F parameter : parameterList)
                    cachedParameters.assign(parameter);
            }
    );

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit(at = "1 - 3")
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            UnitMng.loop(5, (epoch) -> {

                TransactionMng.commit(fetchParameters);

                int id = programContext.nodeID;

                gradients.set(0, id, (float)id + cachedParameters.get(0, id));

                TransactionMng.commit(updateParameters);
            });
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
