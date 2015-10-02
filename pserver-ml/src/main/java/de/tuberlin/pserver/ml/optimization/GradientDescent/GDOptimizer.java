package de.tuberlin.pserver.ml.optimization.GradientDescent;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.controlflow.loop.LoopTermination;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.runtime.SlotContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GDOptimizer implements Optimizer, LoopTermination {

    private static final Logger LOG = LoggerFactory.getLogger(GDOptimizer.class);

    private SlotContext ctx;

    private int maxIterations;

    private int batchSize;

    private double initialLearningRate;

    private boolean shuffle;

    private boolean optimizeIntercept;

    private boolean newtonMethod;

    private LossFunction lossFunction;

    private LearningRateFunction learningRateFunction;

    private int syncMode;

    private boolean converged;

    private double regularization;


    public GDOptimizer(final SlotContext ctx) {
        this.ctx = Preconditions.checkNotNull(ctx);

        this.maxIterations = 100;
        this.batchSize = 1;
        this.initialLearningRate = 1.0;
        this.shuffle = true;
        this.optimizeIntercept = true;
        this.newtonMethod = false;
        this.lossFunction = new LossFunction.GenericLossFunction(
                new PredictionFunction.LinearPrediction(),
                new PartialLossFunction.SquareLoss(),
                new RegularizationFunction.L2Regularization());
        this.learningRateFunction = new LearningRateFunction.ConstantLearningRate();
        this.syncMode = Loop.ASYNC;
        this.regularization = 1e-4;
    }

    public GDOptimizer setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
        return this;
    }

    public GDOptimizer setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public GDOptimizer setInitialLearningRate(double initialLearningRate) {
        this.initialLearningRate = initialLearningRate;
        return this;
    }

    public GDOptimizer setShuffle(boolean shuffle) {
        this.shuffle = shuffle;
        return this;
    }

    public GDOptimizer setLossFunction(LossFunction lossFunction) {
        this.lossFunction = lossFunction;
        return this;
    }

    public GDOptimizer setRegularization(double regularization) {
        this.regularization = regularization;
        return this;
    }

    public GDOptimizer setLearningRateFunction(LearningRateFunction learningRateFunction) {
        this.learningRateFunction = learningRateFunction;
        return this;
    }

    @Override
    public Matrix optimize(Matrix X, Matrix y, Matrix W) throws Exception {

        Matrix.RowIterator XIterator = X.rowIterator();

        converged = false;

        ctx.CF.loop().sync(syncMode).exe(this, (epoch) -> {

            Matrix gradient = new MatrixBuilder().dimension(1, X.cols()).build();

            Matrix batchX = new MatrixBuilder().dimension(batchSize, X.cols()).build();

            Matrix batchY = new MatrixBuilder().dimension(batchSize, y.cols()).build();

            final double learningRate =
                    learningRateFunction.decayLearningRate(epoch, initialLearningRate);

            int count = 0;

            while (XIterator.hasNext()) {

                for (int sample = 0; sample < batchSize; ++sample) {

                    if (shuffle) {
                        XIterator.nextRandom();
                    } else {
                        XIterator.next();
                    }

                    batchX.assignRow(sample, XIterator.get());
                    batchY.assignRow(sample, y.getRow(XIterator.rowNum()));

                    count++;
                }

                gradient = lossFunction.gradient(batchX, batchY, W, regularization, newtonMethod);

                if (!newtonMethod) {
                    gradient.scale(learningRate, gradient);
                }

                W.sub(gradient, W);
            }

            LOG.info("Objective[" + epoch + "]: " + lossFunction.loss(X, y, W, regularization));

            if (epoch >= maxIterations - 1) {
                converged = true;
            }

            XIterator.reset();
        });

        return W;
    }

    @Override
    public boolean terminate() throws Exception {
        return converged;
    }
}
