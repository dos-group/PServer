package de.tuberlin.pserver.examples.experiments.regression;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
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

import java.util.Arrays;


public class LogRegCriteoRM extends Program {

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

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int PER_NODE_DOP =
            (UPDATE_MODEL == UpdateModel.PAR || UPDATE_MODEL == UpdateModel.RED)
                    ? Runtime.getRuntime().availableProcessors() : 1;

    private static final String HOME        = "/Users/Chris/Downloads/model_" + LogRegCriteoRM.class.getSimpleName()
            + "_" + CONSISTENCY_MODEL + "_" + UPDATE_MODEL;


    private static final double STEP_SIZE    = 1e-2;

    private static final int NUM_EPOCHS     = 250;

    private static final String DATA_PATH   = "datasets/svm_train";

    private static final long ROWS          = 80000;

    private static final long COLS          = 1048615;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private LinearModel model;

    private double mle = 0.0;

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

    private static double sigmoid(double z) {
        return 1.0f / (1.0f + Math.exp(-z));
    }

    /*private static double classify(float[] w, float[] x) {
        float logit = 0f;
        for (int i = 0; i < w.length; i++)
            logit += w[i] * x[i];
        return sigmoid(logit);
    }*/

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

                //TODO: random sampling is missing

                atomic(state(W), () ->
                    F.processRows(PER_NODE_DOP, (id, row, valueList, rowStart, rowEnd, colList) -> {

                        float[] w = UPDATE_MODEL == UpdateModel.SEQ
                                ? model.getModel().data
                                : model.getModel(id).data;

                        double z = 0;
                        for (int i = rowStart; i < rowEnd; ++i)
                            z += valueList[i] * w[colList[i]];

                        double label = Y.data[row];

                        double h = sigmoid(z);

                        double f = h - label;

                        for (int j = rowStart; j < rowEnd; ++j) {
                            int ci = colList[j];
                            w[ci] -= STEP_SIZE * valueList[j] * f;
                        }
                    })
                );

                mle = 0;

                F.processRows(PER_NODE_DOP, (id, row, valueList, rowStart, rowEnd, colList) -> {

                    float[] w = UPDATE_MODEL == UpdateModel.SEQ
                            ? model.getModel().data
                            : model.getModel(id).data;

                    double z = 0;
                    for (int i = rowStart; i < rowEnd; ++i)
                        z += valueList[i] * w[colList[i]];

                    double label = Y.data[row];

                    if (label > 0) {
                        mle += Math.log1p(Math.exp(-z));
                    } else {
                        mle += Math.log1p(Math.exp(-z)) + z;
                    }
                });

                System.out.println("MLE[" + epoch + "]: " + mle);

                //
                // Global Model Merging.
                //

                //TransactionMng.commit(merge_W);
                //model.save(epoch);

                System.out.println("FINISHED EPOCH [" + epoch + "]");
            });

        }).postProcess(() -> {
            //if (programContext.node(0)) {
            //    TransactionMng.commit(merge_W);
            //    model.save();
            //    System.out.println("- MERGED FINAL MODELS -");
            //}
        });
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        System.setProperty("global.simNodes", "1");

        PServerExecutor.LOCAL
                .run(LogRegCriteoRM.class)
                .done();
    }
}