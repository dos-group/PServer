package de.tuberlin.pserver.ml.optimization.GradientDescent;

import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.LoopTermination;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.runtime.state.matrix.MatrixBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GDOptimizer implements Optimizer, LoopTermination {

    private static final Logger LOG = LoggerFactory.getLogger(GDOptimizer.class);

    private int maxIterations;

    private int batchSize;

    private float initialLearningRate;

    private boolean shuffle;

    private boolean optimizeIntercept;

    private boolean newtonMethod;

    private LossFunction lossFunction;

    private LearningRateFunction learningRateFunction;

    private int syncMode;

    private boolean converged;

    private float regularization;


    public GDOptimizer() {
        this.maxIterations = 100;
        this.batchSize = 1;
        this.initialLearningRate = 1.0f;
        this.shuffle = true;
        this.optimizeIntercept = true;
        this.newtonMethod = false;
        this.lossFunction = new LossFunction.GenericLossFunction(
                new PredictionFunction.LinearPrediction(),
                new PartialLossFunction.SquareLoss(),
                new RegularizationFunction.L2Regularization());
        this.learningRateFunction = new LearningRateFunction.ConstantLearningRate();
        this.syncMode = Loop.ASYNCHRONOUS;
        this.regularization = 1e-4f;
    }

    public GDOptimizer setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
        return this;
    }

    public GDOptimizer setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public GDOptimizer setInitialLearningRate(float initialLearningRate) {
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

    public GDOptimizer setRegularization(float regularization) {
        this.regularization = regularization;
        return this;
    }

    public GDOptimizer setLearningRateFunction(LearningRateFunction learningRateFunction) {
        this.learningRateFunction = learningRateFunction;
        return this;
    }

    public GDOptimizer setSyncMode(int syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    @Override
    public Matrix32F optimize(Matrix32F X, Matrix32F y, Matrix32F W) throws Exception {

        Matrix32F.RowIterator XIterator = X.rowIterator();

        converged = false;

        UnitMng.loop(this, syncMode, (epoch) -> {

            Matrix32F gradient = new MatrixBuilder().dimension(1, X.cols()).build();

            Matrix32F batchX = new MatrixBuilder().dimension(batchSize, X.cols()).build();

            Matrix32F batchY = new MatrixBuilder().dimension(batchSize, y.cols()).build();

            final float learningRate =
                    learningRateFunction.decayLearningRate(epoch, initialLearningRate);

            while (XIterator.hasNext()) {

                if (batchSize > 1) {

                    for (int sample = 0; sample < batchSize; ++sample) {

                        if (shuffle) {
                            XIterator.nextRandom();
                        } else {
                            XIterator.next();
                        }

                        batchX.assignRow(sample, XIterator.get());
                        batchY.assignRow(sample, y.getRow(XIterator.rowNum()));
                    }

                    gradient = lossFunction.gradient(batchX, batchY, W, regularization, newtonMethod);
                } else {

                    if (shuffle) {
                        XIterator.nextRandom();
                    } else {
                        XIterator.next();
                    }

                    gradient = lossFunction.gradient(XIterator.get(), y.getRow(XIterator.rowNum()),
                            W, regularization, newtonMethod);
                }

                if (!newtonMethod) {
                    gradient.scale(learningRate, gradient);
                }

                W.sub(gradient, W);
            }

            //LOG.info("Objective[" + epoch + "]: " + lossFunction.loss(X, y, W, regularization));

            if (epoch >= maxIterations - 1) {
                converged = true;
            }

            XIterator.reset();

            System.out.println("EPOCH => " + epoch);
        });

        return W;
    }

    @Override
    public boolean terminate() throws Exception {
        return converged;
    }
}