package de.tuberlin.pserver.examples.playground;

/*public final class AsyncHogwildSGDWithMonitoringTestJob extends PServerJob {

    // ---------------------------------------------------
    // Inner Class.
    // ---------------------------------------------------

    public static final class ExecutionMonitorMsg {

        public final double validationError;

        public ExecutionMonitorMsg(final double validationError) {
            this.validationError = validationError;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long MONITOR_INTERVAL = 5000;

    private volatile static int finishCounter = 0;

    // ---------------------------------------------------

    private Vector weightsSnapshot = new DVector(1000);

    private Vector changeVector = new DVector(1000);

    private final OptimizerObserver observer = (epoch, weights, gradientSum) -> {

        for (int i = 0; i < weights.size(); ++i) {
            changeVector.set(i, ((weightsSnapshot.get(i) - weights.get(i)) / weights.get(i)) > 0.0001 ? 1.0 : 0.0);
        }

        weightsSnapshot.assign(weights);

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        for (double weight : changeVector.toArray())
            System.out.print(numberFormat.format(weight) + "\t | ");
        System.out.println();
    };

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void prologue() {

        dataManager.loadDMatrix("datasets/sparse_dataset.csv");

        //dataManager.loadDMatrix("datasets/sparse_validation_dataset.csv");

        dataManager.createLocalVector("weight-vector", 1000, Vector.VectorType.ROW_VECTOR);
    }

    @Override
    public void compute() {

        //if (ctx.threadID == 1) {

        //    monitorExecution();

        //} else {

            final Vector weights = dataManager.getLocalVector("weight-vector");

            final Matrix trainingData = dataManager.getLocalMatrix("sparse_dataset.csv");

            final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

            final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

            final Optimizer optimizer = new SGDOptimizer(SGDOptimizer.TYPE.SGD_SIMPLE)
                    .setNumberOfIterations(100000)
                    .setLearningRate(0.00005)
                    .setLossFunction(new LossFunction.GenericLossFunction(predictionFunction, partialLossFunction))
                    .setGradientStepFunction(new GradientStepFunction.AtomicGradientStep())
                    .setLearningRateDecayFunction(null)
                    .setWeightsObserver(observer, 200)
                    .setRandomShuffle(true);

            final Matrix.RowIterator dataIterator = dataManager.threadPartitionedRowIterator(trainingData);

            optimizer.optimize(weights, dataIterator);

            ++finishCounter;

            result(weights);
        //}
    }

    /*public void monitorExecution() {

        final Matrix validationData = dataManager.getLocalMatrix("sparse_validation_dataset.csv");

        final Matrix.RowIterator validationDataIterator = validationData.rowIterator();

        final PredictionFunction predictionFunction = new PredictionFunction.LinearPredictionFunction();

        final PartialLossFunction partialLossFunction = new PartialLossFunction.SquareLoss();

        final int numFeatures = (int)validationDataIterator.numCols() - 1;

        while (finishCounter < 1) {

            // Calculate validation error with weightsSnapshot of the model.
            Vector snapshotWeights = dataManager.getLocalVector("weight-vector");

            double loss = 0.0;

            while (validationDataIterator.hasNextRow()) {

                validationDataIterator.nextRow();

                final Vector features = validationDataIterator.getAsVector(0, numFeatures);

                final double p = predictionFunction.predict(features, snapshotWeights);

                final double l = validationDataIterator.getValueOfColumn((int) validationDataIterator.numCols() - 1);

                loss += partialLossFunction.loss(p, l);
            }

            loss = loss * (1.0 / validationDataIterator.numRows());

            LOG.info("VALIDATION ERROR = " + loss);

            validationDataIterator.reset();

            try {
                Thread.sleep(MONITOR_INTERVAL);
            } catch (InterruptedException e) {}
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> weights = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(AsyncHogwildSGDWithMonitoringTestJob.class, 2) // <-- enable multi-threading, 2 threads per compute node.
                .results(weights)
                .done();

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        weights.forEach(
                r -> r.forEach(
                        w -> {
                            for (double weight : ((Vector)w).toArray())
                                System.out.print(numberFormat.format(weight) + "\t | ");
                            System.out.println();
                        }
                )
        );
    }
}*/