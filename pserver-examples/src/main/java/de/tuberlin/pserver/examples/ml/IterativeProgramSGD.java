package de.tuberlin.pserver.examples.ml;

import de.tuberlin.pserver.app.IterativeProgram;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.SGD.SGDOptimizer;

public class IterativeProgramSGD extends IterativeProgram {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final GeneralLinearModel model;

    private final Optimizer optimizer;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public IterativeProgramSGD() {

        internalSync(SynchronizationMode.ASYNCHRONOUS);

        externalSync(SynchronizationMode.ASYNCHRONOUS);

        numIterations(150);

        this.model = new GeneralLinearModel("model1", 15);

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        this.optimizer = new SGDOptimizer(ctx, SGDOptimizer.TYPE.SGD_SIMPLE, false)
                .setLearningRate(0.0005)
                .setLossFunction(new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction))
                .setGradientStepFunction(new GradientStepFunction.SimpleGradientStep())
                .setLearningRateDecayFunction(null);
    }

    // ---------------------------------------------------
    // Lifecycle.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        model.createModel(ctx);

        dataManager.loadAsMatrix("datasets/sparse_dataset.csv", GenerateLocalTestData.ROWS_SPARSE_DATASET, GenerateLocalTestData.COLS_SPARSE_DATASET);
    }

    @Override
    public void iterate() {

        // TODO: implement optimize call on a single sample.

    }
}
