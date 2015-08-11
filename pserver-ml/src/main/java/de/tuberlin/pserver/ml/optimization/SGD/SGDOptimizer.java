package de.tuberlin.pserver.ml.optimization.SGD;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.ml.common.LabeledVector;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.runtime.SlotContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGDOptimizer implements Optimizer {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static enum TYPE {

        SGD_SIMPLE,

        SGD_MINIBATCH,

        SGD_AVERAGED // from [http://research.microsoft.com/pubs/192769/tricks-2012.pdf]
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(SGDOptimizer.class);

    private boolean useLogging;

    //private int labelColumnIndex = -1;

    private final SlotContext ctx;

    private TYPE sgdType;

    private double alpha;

    private int numIterations;

    private LossFunction lossFunction;

    private GradientStepFunction gradientStepFunction;

    private DecayFunction decayFunction;

    private Observer observer;

    private int period;

    private boolean useRandomShuffle;

    private boolean observerThreadSyncedModel;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SGDOptimizer(final SlotContext ctx, final TYPE type, final boolean useLogging) {

        this.ctx = Preconditions.checkNotNull(ctx);

        this.sgdType = Preconditions.checkNotNull(type);

        this.useLogging = useLogging;

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        this.lossFunction = new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction);

        this.gradientStepFunction = new GradientStepFunction.SimpleGradientStep();

        this.decayFunction = new DecayFunction.SimpleDecay();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public GeneralLinearModel optimize(GeneralLinearModel model, Matrix.RowIterator dataIterator) {
        Preconditions.checkNotNull(model);

        model.fetchModel(ctx);

        switch (sgdType) {
            case SGD_SIMPLE:
                return simple_sgd(model, dataIterator);
            case SGD_MINIBATCH:
                throw new UnsupportedOperationException();
            case SGD_AVERAGED:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public Vector optimize(GeneralLinearModel model, Vector example) {
        return null;
    }

    // ---------------------------------------------------

    public SGDOptimizer setLearningRate(final double alpha) { this.alpha = alpha; return this; }

    public SGDOptimizer setLearningRateDecayFunction(final DecayFunction decayFunction) { this.decayFunction = decayFunction; return this; }

    public SGDOptimizer setNumberOfIterations(final int numIterations) { this.numIterations = numIterations; return this; }

    public SGDOptimizer setLossFunction(final LossFunction lossFunction) { this.lossFunction = lossFunction; return this; }

    public SGDOptimizer setGradientStepFunction(final GradientStepFunction gradientStepFunction) { this.gradientStepFunction = gradientStepFunction; return this; }

    public SGDOptimizer setWeightsObserver(final Observer observer, final int period, final boolean observerThreadSyncedModel) {
        this.observer = observer;
        this.period = period;
        this.observerThreadSyncedModel = observerThreadSyncedModel;
        return this;
    }

    public SGDOptimizer setRandomShuffle(final boolean useRandomShuffle) { this.useRandomShuffle = useRandomShuffle; return this; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    private GeneralLinearModel simple_sgd(final GeneralLinearModel model, final Matrix.RowIterator dataIterator) {

        final int numFeatures = (int)dataIterator.numCols() - 1;

        model.startTraining();

        for (int epoch = 0; epoch < numIterations; ++epoch) {

            while (dataIterator.hasNextRow()) {

                if (useRandomShuffle)
                    dataIterator.nextRandomRow();
                else
                    dataIterator.nextRow();

                final double label = dataIterator.getValueOfColumn((int) dataIterator.numCols() - 1);

                final Vector featureVector = dataIterator.getAsVector(0, numFeatures);

                final LabeledVector labeledVector = new LabeledVector(label, featureVector);

                final Vector gradient = lossFunction.gradient(labeledVector, model.getWeights());

                //gradientSum.add(gradient);

                model.updateModel(gradientStepFunction.takeStep(model.getWeights(), gradient, alpha));
            }

            if (decayFunction != null) {
                alpha = decayFunction.decayLearningRate(epoch, alpha, alpha);
            }

            dataIterator.reset();

            if (epoch % period == 0) {
                    updateObserver(model);
            }
        }

        model.stopTraining();

        return model;
    }

    private void updateObserver(final GeneralLinearModel model) {
        /*if (ctx.slotID == 0) {
            final Vector[] gradientSums = new Vector[ctx.jobContext.numOfSlots];
            for (int i = 0; i < ctx.jobContext.numOfSlots; ++i) {
                gradientSums[i] = gradientSum;
                state.gradientSum.assign(0.0);
                if (useLogging) {
                    LOG.info(state.toString());
                }
            }
            if (useLogging) {
                LOG.info("++");
            }
            observer.update(state.epoch, model.getWeights(), null);
        }*/
    }
}
