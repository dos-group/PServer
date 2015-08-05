package de.tuberlin.pserver.ml.optimization.SGD;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.app.ExecutionManager;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.common.LabeledVector;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.utils.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CyclicBarrier;

public class SGDOptimizer implements Optimizer {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    // Keep the state of a single optimizer instance.
    public static final class SGDOptimizerState {

        private static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

        public int threadID;

        public int epoch;

        public double alpha;

        public Vector gradientSum;

        @Override
        public String toString() { return "\nSGDOptimizerState " + gson.toJson(this); }
    }

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

    private final InstanceContext ctx;

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

    private SGDOptimizerState state;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SGDOptimizer(final InstanceContext ctx, final TYPE type, final boolean useLogging) {

        this.ctx = Preconditions.checkNotNull(ctx);

        this.sgdType = Preconditions.checkNotNull(type);

        this.useLogging = useLogging;

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        this.lossFunction = new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction);

        this.gradientStepFunction = new GradientStepFunction.SimpleGradientStep();

        this.decayFunction = new DecayFunction.SimpleDecay();

        this.state = new SGDOptimizerState();
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

    @Override
    public void register() {
        ctx.jobContext.executionManager.registerAlgorithm(this.getClass(), state);
    }

    @Override
    public void unregister() {
        ctx.jobContext.executionManager.unregisterAlgorithm();
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

        if (ctx.instanceID == 0) {
            if (observerThreadSyncedModel) {
                ctx.jobContext.executionManager.putJobScope("local-sgd-barrier",
                        new CyclicBarrier(ctx.jobContext.numOfInstances, () -> updateObserver(model)));
            }
        }

        state.threadID = ctx.instanceID;

        state.alpha = alpha;

        final int numFeatures = (int)dataIterator.numCols() - 1;

        state.gradientSum = new DVector(model.getWeights().length(), Vector.Layout.COLUMN_LAYOUT);

        model.startTraining();

        for (state.epoch = 0; state.epoch < numIterations; ++state.epoch) {

            while (dataIterator.hasNextRow()) {

                if (useRandomShuffle)
                    dataIterator.nextRandomRow();
                else
                    dataIterator.nextRow();

                final double label = dataIterator.getValueOfColumn((int) dataIterator.numCols() - 1);

                final Vector featureVector = dataIterator.getAsVector(0, numFeatures);

                final LabeledVector labeledVector = new LabeledVector(label, featureVector);

                final Vector gradient = lossFunction.gradient(labeledVector, model.getWeights());

                state.gradientSum.add(gradient);

                model.updateModel(gradientStepFunction.takeStep(model.getWeights(), gradient, alpha));
            }

            if (decayFunction != null) {
                state.alpha = decayFunction.decayLearningRate(state.epoch, alpha, state.alpha);
            }

            dataIterator.reset();

            if (observer != null && state.epoch % period == 0) {
                if (observerThreadSyncedModel) {
                    try {
                        ((CyclicBarrier) ctx.jobContext.executionManager.getJobScope("local-sgd-barrier")).await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    updateObserver(model);
                }
            }
        }

        model.stopTraining();

        return model;
    }

    private void updateObserver(final GeneralLinearModel model) {
        if (ctx.instanceID == 0) {
            ExecutionManager.ExecutionDescriptor[] descriptors
                    = ctx.jobContext.executionManager.getExecutionDescriptors(ctx.jobContext.jobUID);
            final Vector[] gradientSums = new Vector[ctx.jobContext.numOfInstances];
            for (int i = 0; i < ctx.jobContext.numOfInstances; ++i) {
                final SGDOptimizerState state = (SGDOptimizerState)descriptors[i].stateObj;
                gradientSums[i] = state.gradientSum;
                state.gradientSum.assign(0.0);
                if (useLogging) {
                    LOG.info(state.toString());
                }
            }
            if (useLogging) {
                LOG.info("++");
            }
            observer.update(state.epoch, model.getWeights(), null);
        }
    }
}
