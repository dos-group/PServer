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
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;


public class ParameterServerSketch extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    // ParamServer runs on Node 0, 1.

    @State(at = "0", scope = Scope.PARTITIONED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 10, cols = 10000)
    public Matrix32F parameter;

    // Worker runs on Node 2, 3.

    @State(at = "2, 3", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.SPARSE_FORMAT, rows = 10, cols = 10000)
    public Matrix32F parameterCache;

    @State(at = "2, 3", scope = Scope.REPLICATED, matrixFormat = MatrixFormat.DENSE_FORMAT, rows = 10, cols = 10000)
    public Matrix32F gradients;

    @State(scope = Scope.PARTITIONED, rows = 300000, cols = 10000, path = "datasets/XXX", fileFormat = FileFormat.SVM_FORMAT)
    public Matrix32F data;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(src = "gradients", dst = "parameter", type = TransactionType.PUSH)
    public final TransactionDefinition parameterUpdate = new TransactionDefinition(

            (Update<Matrix32F>) (remoteGradients, parameter) -> {
                for (Matrix32F grad : remoteGradients) {
                    parameter.add(grad, parameter);
                }
            }
    );

    @Transaction(src = "parameter", dst = "parameterCache", type = TransactionType.PULL)
    public final TransactionDefinition parameterPull = new TransactionDefinition(

            (Update<Matrix32F>) (remoteParameters, parameterCache) -> {
                parameterCache.assign(remoteParameters.get(0));
            }
    );

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit(at = "2, 3")
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            /*new Timer(true).scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        TransactionMng.commit(parameterPull);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }, 0, 10 * 1000);*/

            UnitMng.loop(15,  Loop.BULK_SYNCHRONOUS, (e) -> {

                Matrix32F.RowIterator it = data.rowIterator();

                while (it.hasNext()) {

                    it.next();

                    //
                    // update logic....
                    //

                    TransactionMng.commit(parameterPull, new int[] {0, 345, 5465, 2457});
                }

                TransactionMng.commit(parameterUpdate);
            });
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
