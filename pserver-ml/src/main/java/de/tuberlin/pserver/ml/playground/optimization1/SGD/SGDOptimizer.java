package de.tuberlin.pserver.ml.playground.optimization1.SGD;


public class SGDOptimizer {} /*implements Optimizer {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    // Keep the state of a single optimizer instance.
    public static final class SGDOptimizerState {

        private static Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

        public int instanceID;

        public int epoch;

        public double alpha;

        public Matrix gradientSum;

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

    private final PServerContext instanceContext;

    private TYPE sgdType;

    private double alpha;

    private int numIterations;

    private int period;

    private boolean useRandomShuffle;

    private boolean observerThreadSyncedModel;


    private LossFunction lossFunction;

    private GradientStepFunction gradientStepFunction;

    private DecayFunction decayFunction;

    private Observer observer;

    private SGDOptimizerState state;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SGDOptimizer(final PServerContext instanceContext, final TYPE type, final boolean useLogging) {

        this.instanceContext = Preconditions.checkNotNull(instanceContext);

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

        model.fetchModel(instanceContext);

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
    public void register() {
        instanceContext.executionManager.registerAlgorithm(this.getClass(), state);
    }

    @Override
    public void unregister() {
        instanceContext.executionManager.unregisterAlgorithm();
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

        if (instanceContext.instanceID == 0) {
            if (observerThreadSyncedModel) {
                instanceContext.executionManager.putJobScope("local-sgd-barrier",
                        new CyclicBarrier(instanceContext.perNodeParallelism, () -> updateObserver(model)));
            }
        }

        state.instanceID = instanceContext.instanceID;

        state.alpha = alpha;

        final int numFeatures = (int)dataIterator.numCols() - 1;

        state.gradientSum = new DMatrix(model.getWeights().numRows(), model.getWeights().numCols());

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

                final Matrix gradient = lossFunction.gradient(labeledVector, model.getWeights());

                state.gradientSum.add(gradient);

                final Matrix update = gradientStepFunction.takeStep(model.getWeights(), gradient, alpha);

                model.updateModel(update);
            }

            if (decayFunction != null) {
                state.alpha = decayFunction.decayLearningRate(state.epoch, alpha, state.alpha);
            }

            dataIterator.reset();

            if (observer != null && state.epoch % period == 0) {
                if (observerThreadSyncedModel) {
                    try {
                        ((CyclicBarrier) instanceContext.executionManager.getJobScope("local-sgd-barrier")).await();
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
        if (instanceContext.instanceID == 0) {
            ExecutionManager.ExecutionDescriptor[] descriptors
                    = instanceContext.executionManager.getExecutionDescriptors(instanceContext.jobUID);
            final Matrix[] gradientSums = new Matrix[instanceContext.perNodeParallelism];
            for (int i = 0; i < instanceContext.perNodeParallelism; ++i) {
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
}*/
