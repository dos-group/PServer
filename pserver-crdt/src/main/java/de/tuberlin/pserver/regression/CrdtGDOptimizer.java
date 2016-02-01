package de.tuberlin.pserver.regression;

import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.unit.controlflow.loop.LoopTermination;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.ml.optimization.*;
import de.tuberlin.pserver.runtime.state.matrix.MatrixBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CrdtGDOptimizer implements Optimizer, LoopTermination {

    private static final Logger LOG = LoggerFactory.getLogger(CrdtGDOptimizer.class);

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


    public CrdtGDOptimizer() {
        this.maxIterations = 1000;
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

    public CrdtGDOptimizer setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
        return this;
    }

    public CrdtGDOptimizer setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public CrdtGDOptimizer setInitialLearningRate(float initialLearningRate) {
        this.initialLearningRate = initialLearningRate;
        return this;
    }

    public CrdtGDOptimizer setShuffle(boolean shuffle) {
        this.shuffle = shuffle;
        return this;
    }

    public CrdtGDOptimizer setLossFunction(LossFunction lossFunction) {
        this.lossFunction = lossFunction;
        return this;
    }

    public CrdtGDOptimizer setRegularization(float regularization) {
        this.regularization = regularization;
        return this;
    }

    public CrdtGDOptimizer setLearningRateFunction(LearningRateFunction learningRateFunction) {
        this.learningRateFunction = learningRateFunction;
        return this;
    }

    public CrdtGDOptimizer setSyncMode(int syncMode) {
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
            int count = 0;

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

               if(W instanceof NoSessionAvgRegressionDenseMatrix32F) {
                   ((NoSessionAvgRegressionDenseMatrix32F)W).subAndBroadcast(gradient, W);
                   //((NoSessionAvgRegressionDenseMatrix32F)W).applyWaitingOperations();
                   System.out.println(count + " Model " + W);
                   System.out.println(count + " Gradient " + gradient);
                   count++;
                }
               else if(W instanceof ExactAvgRegressionDenseMatrix32F) {
                   ((ExactAvgRegressionDenseMatrix32F)W).subAndBroadcast(gradient, W);
                   //System.out.println(count + " " + W);
                   count++;
               }
               else {
                    W.sub(gradient, W);
                }
            }

            System.out.println("Loop Count: " + count);
            LOG.info("Objective[" + epoch + "]: " + lossFunction.loss(X, y, W, regularization));

            // End if the gradient is very small !?
            if (epoch >= maxIterations - 1) {
                converged = true;
                ((AbstractAvgReplicatedMatrix)W).setConverged();
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
