package de.tuberlin.pserver.benchmarks.criteo.logreg.experiments;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.commons.config.ConfigLoader;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.TransactionMng;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.annotations.TransactionType;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.ml.LinearModel;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.Load;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public class Exp_LRRM extends Program {

    // ---------------------------------------------------
    // Experiment Configurations.
    // ---------------------------------------------------

    public enum ConsistencyModel {
        TAP, // Totally Asynchronous Parallel
        BSP  // Bulk Synchronous Parallel
    }

    public enum UpdateModel {
        SEQ, // Sequential
        PAR, // Parallel (HogWild)
        RED  // Parallel Reduce
    }

    // ---------------------------------------------------
    // Experiment Setup.
    // ---------------------------------------------------

    private static final ConsistencyModel CONSISTENCY_MODEL = ConsistencyModel.TAP;

    private static final UpdateModel UPDATE_MODEL = UpdateModel.SEQ;

    private static final int PER_NODE_DOP =
            (UPDATE_MODEL == UpdateModel.PAR || UPDATE_MODEL == UpdateModel.RED)
            ? Runtime.getRuntime().availableProcessors() : 1;

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String HOME        = "/home/tobias.herb/model_" + Exp_LRRM.class.getSimpleName()
            + "_" + CONSISTENCY_MODEL + "_" + UPDATE_MODEL;

    private static final String DATA_PATH   = "/criteo/criteo_train";

    private static final int NUM_EPOCHS     = 15;

    private static final float STEP_SIZE    = 1.0f;

    private static final long ROWS          = 195841983;

    private static final long COLS          = 1048615;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private LinearModel model;

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = 1)
    public DenseMatrix32F Y;

    @Load(filePath = DATA_PATH, labels = "Y")
    @Matrix(scheme = DistScheme.H_PARTITIONED, rows = ROWS, cols = COLS)
    public CSRMatrix32F F;

    @Matrix(scheme = DistScheme.REPLICATED, rows = 1, cols = COLS)
    public DenseMatrix32F W;

    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "W", type = TransactionType.PULL)
    public final TransactionDefinition merge_W = new TransactionDefinition(
            (Update<Matrix32F>) (requestObj, r, l) -> model.merge(r)
    );

    // ---------------------------------------------------
    // Units.
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {

        lifecycle.preProcess(() -> {

            model = new LinearModel(PER_NODE_DOP, W, HOME + getClass().getName());

        }).process(() -> {

            UnitMng.loop(NUM_EPOCHS, epoch -> {

                if (CONSISTENCY_MODEL == ConsistencyModel.BSP)
                    UnitMng.barrier(UnitMng.GLOBAL_BARRIER);

                //
                // Local Model Update.
                //

                model.nextEpoch();

                atomic(state(W), () ->
                    F.processRows(PER_NODE_DOP, (id, row, valueList, rowStart, rowEnd, colList) -> {

                        if (row > 0 && row < 10) {
                            System.out.println("Label = " + Y.data[row]);
                            for (int i = rowStart; i < rowEnd; ++i) {
                                System.out.println(colList[i] + ":" + valueList[i]);
                            }
                            System.out.println("--------------");
                        }

                        float[] w = UPDATE_MODEL == UpdateModel.RED
                                ? model.getModel(id).data
                                : model.getModel().data;

                        float[] d = model.getDerivative(id).data;
                        float[] g = model.getGradient(id).data;

                        float yPredict = 0;
                        for (int i = rowStart; i < rowEnd; ++i) {
                            yPredict += valueList[i] * w[colList[i]];
                        }

                        float f = Y.data[row] - yPredict;
                        for (int j = rowStart; j < rowEnd; ++j) {
                            int ci = colList[j];
                            d[ci] = valueList[ci] * f;
                            g[ci] += d[ci] * STEP_SIZE;
                            w[ci] -= g[ci];
                        }

                        if (UPDATE_MODEL == UpdateModel.RED)
                            model.sync();
                    })
                );

                //
                // Global Model Merging.
                //
                TransactionMng.commit(merge_W);
                model.save(epoch);
                System.out.println("FINISHED EPOCH [" + epoch + "]");
            });

        }).postProcess(() -> {
            if (programContext.node(0)) {
                TransactionMng.commit(merge_W);
                model.save();
                System.out.println("- MERGED FINAL MODELS -");
            }
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        PServerExecutor.DISTRIBUTED
                .run(ConfigLoader.loadResource("distributed.conf"), Exp_LRRM.class)
                .done();
    }
}
