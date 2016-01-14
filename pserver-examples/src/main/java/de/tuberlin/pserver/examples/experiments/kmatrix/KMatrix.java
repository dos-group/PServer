package de.tuberlin.pserver.examples.experiments.kmatrix;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class KMatrix extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @State(scope = Scope.REPLICATED,
            rows = 4,
            cols = 4,
            format = Format.DENSE_FORMAT
    )
    public Matrix32F data;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "data", type = TransactionType.PUSH)
    public final TransactionDefinition sync = new TransactionDefinition(

            (Update<Matrix32F>) (remoteUpdates, localState) -> {

            }
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit(at = "0")
    public void schedule(final Lifecycle lifecycle) {

        int n = 3;//Integer.parseInt(args[0]);
        int k = 3;//Integer.parseInt(args[1]);
        int chunk_size = 1000;

        Matrix.NonsingularIterator it = new Matrix.NonsingularIterator(n, k);
        it.generate_next();
        int i;
        int chunk_id = 0;

        List<double[]> vecs = new ArrayList<>();
        while (!it.is_done()) {
            Matrix start = new Matrix(it.matrix());
            for (i = 0; i < chunk_size && !it.is_done(); ++i) {
                it.generate_next();
            }
            vecs.add(new double[] {chunk_id, n, k, start.get_id().doubleValue(), i});
            ++chunk_id;
        }

    }

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            int n = 3; //Integer.parseInt(args[1]);
            int k = 3; //Integer.parseInt(args[2]);
            BigInteger start_id = new BigInteger("6643");
            int chunk_size = 10;

            Algorithm.get_cycles(n, k, start_id, chunk_size);


            //UnitMng.loop(10, Loop.BULK_SYNCHRONOUS, (iteration) -> {


            //});
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("simulation.numNodes", "3");
        PServerExecutor.LOCAL
                .run(KMatrix.class)
                .done();
    }
}
