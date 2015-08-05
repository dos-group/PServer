package de.tuberlin.pserver.ml.algorithms.linreg;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.ml.algorithms.MLAlgorithm;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.ml.optimization.SGD.SGDOptimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinRegAlgorithm extends MLAlgorithm<LinRegModel> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LinRegAlgorithm.class);

    private Optimizer optimizer;

    private final Observer observer = (epoch, weights, gradient) -> {
        LOG.info("EPOCH = " + epoch);
    };

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LinRegAlgorithm(final InstanceContext ctx) {
        super(ctx);

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        this.optimizer = new SGDOptimizer(ctx, SGDOptimizer.TYPE.SGD_SIMPLE, true)
            .setNumberOfIterations(1000)
            .setLearningRate(0.005)
            .setLossFunction(new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction))
            .setGradientStepFunction(new GradientStepFunction.SimpleGradientStep())
            .setLearningRateDecayFunction(null)
            .setWeightsObserver(observer, 100, true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void train(final LinRegModel model, final Matrix data) {

        final Matrix.RowIterator dataIterator = ctx.jobContext.dataManager.createThreadPartitionedRowIterator(data);

        optimizer.optimize(model, dataIterator);
    }

    @Override
    public void register() { optimizer.register(); }

    @Override
    public void unregister() { optimizer.unregister(); }

    // ---------------------------------------------------

    public void setOptimizer(final Optimizer optimizer) { this.optimizer = Preconditions.checkNotNull(optimizer); }
}
