package de.tuberlin.pserver.ml.optimization;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
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

    //private int labelColumnIndex = -1;

    private TYPE sgdType;

    private double alpha;

    private int numIterations;

    private LossFunction lossFunction;

    private GradientStepFunction gradientStepFunction;

    private DecayFunction decayFunction;

    private WeightsObserver weightsObserver;

    private boolean useRandomShuffle;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SGDOptimizer(final TYPE type) {

        this.sgdType = Preconditions.checkNotNull(type);

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        this.lossFunction = new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction);

        this.gradientStepFunction = new GradientStepFunction.SimpleGradientStep();

        this.decayFunction = new DecayFunction.SimpleDecay();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Vector optimize(Vector weights, final Matrix.RowIterator dataIterator) {
        switch (sgdType) {
            case SGD_SIMPLE:
                return simple_sgd(weights, dataIterator);
            case SGD_MINIBATCH:
                throw new UnsupportedOperationException();
            case SGD_AVERAGED:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalStateException();
        }
    }

    // ---------------------------------------------------

    public SGDOptimizer setLearningRate(final double alpha) { this.alpha = alpha; return this; }

    public SGDOptimizer setLearningRateDecayFunction(final DecayFunction decayFunction) { this.decayFunction = decayFunction; return this; }

    public SGDOptimizer setNumberOfIterations(final int numIterations) { this.numIterations = numIterations; return this; }

    public SGDOptimizer setLossFunction(final LossFunction lossFunction) { this.lossFunction = lossFunction; return this; }

    public SGDOptimizer setGradientStepFunction(final GradientStepFunction gradientStepFunction) { this.gradientStepFunction = gradientStepFunction; return this; }

    public SGDOptimizer setWeightsObserver(final WeightsObserver weightsObserver) { this.weightsObserver = weightsObserver; return this; }

    public SGDOptimizer setRandomShuffle(final boolean useRandomShuffle) { this.useRandomShuffle = useRandomShuffle; return this; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    private Vector simple_sgd(Vector weights, final Matrix.RowIterator dataIterator) {

        double current_alpha = alpha;

        final int numFeatures = (int)dataIterator.numCols() - 1;

        for (int epoch = 0; epoch < numIterations; ++epoch) {

            while (dataIterator.hasNextRow()) {

                if (useRandomShuffle)
                    dataIterator.nextRandomRow();
                else
                    dataIterator.nextRow();

                final double label = dataIterator.getValueOfColumn((int) dataIterator.numCols() - 1);

                final Vector featureVector = dataIterator.getAsVector(0, numFeatures);

                final LabeledVector labeledVector = new LabeledVector(label, featureVector);

                final Vector gradient = lossFunction.gradient(labeledVector, weights);

                weights = gradientStepFunction.takeStep(weights, gradient, alpha);
            }

            if (decayFunction != null) {
                current_alpha = decayFunction.decayLearningRate(epoch, alpha, current_alpha);
            }

            dataIterator.reset();

            if (weightsObserver != null) {
                weightsObserver.weightsUpdate(epoch, weights);
            }
        }

        return weights;
    }
}
