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

    private static final String HOME        = "/home/tobias.herb/model_" + LogRegCriteoRM.class.getSimpleName()
            + "_" + CONSISTENCY_MODEL + "_" + UPDATE_MODEL;


    private static final double STEP_SIZE    = 0.0001;

    private static final int NUM_EPOCHS     = 15;

    private static final String DATA_PATH   = "datasets/onelinemtx";

    private static final long ROWS          = 1;

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

                mle = 0;
                model.nextEpoch();

                atomic(state(W), () ->
                    F.processRows(PER_NODE_DOP, (id, row, valueList, rowStart, rowEnd, colList) -> {

                        float[] w = UPDATE_MODEL == UpdateModel.SEQ
                                ? model.getModel().data
                                : model.getModel(id).data;

                        System.out.println("TEST");

                        // z = W^T * x
                        /*double z = 0;
                        for (int i = rowStart; i < rowEnd; ++i)
                            z += valueList[i] * w[colList[i]];

                        // h(x) = 1 / (1 + exp(-z))
                        // gradient = (y(x) - h(x)) * x
                        double f = Y.data[row] - (1.0 / (1.0 + Math.exp(-1.0 * z)));

                        for (int j = rowStart; j < rowEnd; ++j) {
                            int ci = colList[j];
                            w[ci] += STEP_SIZE * valueList[j] * f; <----------
                        }*/

                        /*float logit = 0f;
                        for (int i = rowStart; i < rowEnd; i++)
                            logit += w[colList[i]] * valueList[i];
                        double predicted = sigmoid(logit);

                        double label = Y.data[row];
                        for (int i = rowStart; i < rowEnd; i++)
                            w[colList[i]] = (float)(w[colList[i]] + STEP_SIZE * (label - predicted) * valueList[i]);

                        if (UPDATE_MODEL == UpdateModel.RED)
                            model.sync();

                        mle += Y.data[row] * Math.log(predicted) + (1 - Y.data[row]) * Math.log(1 - predicted);*/

                        //if (row % 50 == 0)
                        //    System.out.println("row = " + row + " => mle = " + mle);
                    })
                );

                System.out.println("mle[" + epoch + "] -> " + mle);

                //
                // Global Model Merging.
                //

                //TransactionMng.commit(merge_W);
                //model.save(epoch);
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