package de.tuberlin.pserver.examples.experiments.paramsrv;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.types.common.FileFormat;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;
import de.tuberlin.pserver.types.metadata.DistScheme;


public class ParameterServerSketch extends Program {

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    // ParamServer runs on Node 0, 1.

    @Matrix(scheme = DistScheme.H_PARTITIONED, at = "0", type = MatrixType.DENSE_FORMAT, rows = 10, cols = 10000)
    public Matrix32F parameter;

    // Worker runs on Node 2, 3.

    @Matrix(scheme = DistScheme.REPLICATED, at = "2, 3", type = MatrixType.SPARSE_FORMAT, rows = 10, cols = 10000)
    public Matrix32F parameterCache;

    @Matrix(scheme = DistScheme.REPLICATED, at = "2, 3", type = MatrixType.DENSE_FORMAT, rows = 10, cols = 10000)
    public Matrix32F gradients;

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = 300000, cols = 10000, path = "datasets/XXX", format = FileFormat.SVM_FORMAT)
    public Matrix32F data;

    // ---------------------------------------------------
    // Transaction.
    // ---------------------------------------------------

    @Transaction(src = "gradients", dst = "parameter", type = TransactionType.PUSH)
    public final TransactionDefinition parameterUpdate = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, remoteGradients, parameter) -> {
                for (Matrix32F grad : remoteGradients) {
                    parameter.add(grad, parameter);
                }
            }
    );

    @Transaction(src = "parameter", dst = "parameterCache", type = TransactionType.PULL)
    public final TransactionDefinition parameterPull = new TransactionDefinition(

            (Update<Matrix32F>) (requestObj, remoteParameters, parameterCache) -> {
                parameterCache.assign(remoteParameters.get(0));
            }
    );

    // ---------------------------------------------------
    // Unit.
    // ---------------------------------------------------

    @Unit(at = "2, 3")
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

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
