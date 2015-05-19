package de.tuberlin.pserver.ml.optimization.gradientdescent;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerContext;
import de.tuberlin.pserver.experimental.old.DMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SGDBase {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(SGDBase.class);

    protected LossFunction lossFunction;

    protected double alpha;

    protected int numIterations;

    protected int instanceID;

    protected WeightsUpdater weightsUpdater;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SGDBase(final PServerContext ctx) { setInstanceID(Preconditions.checkNotNull(ctx).instanceID); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public SGDBase setLossFunction(final LossFunction lossFunction) { this.lossFunction = lossFunction; return this; }

    public SGDBase setLearningRate(final double alpha) { this.alpha = alpha; return this; }

    public SGDBase setNumberOfIterations(final int numIterations) { this.numIterations = numIterations; return this; }

    public SGDBase setInstanceID(final int instanceID) { this.instanceID = instanceID; return this; }

    public SGDBase setWeightsUpdater(final WeightsUpdater updater) { this.weightsUpdater = updater; return this; }

    // ---------------------------------------------------

    public LossFunction getLossFunction() { return lossFunction; }

    public double getLearningRate() { return alpha; }

    public int getNumIterations() { return numIterations; }

    public int getInstanceID() { return instanceID; }

    public WeightsUpdater getWeightsUpdater() { return weightsUpdater; }

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract DMatrix predict(final DMatrix weights, final DMatrix data);

    public abstract DMatrix fit(final DMatrix weights, final DMatrix data, final int labelIndex);

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    protected DMatrix doPlainSGD(final DMatrix weights,
                                 final DMatrix data,
                                 final int labelIndex,
                                 final LossFunction lossFunction,
                                 final double alpha,
                                 final int numIterations) {

        boolean first = true;
        int numFeatures = 0;
        int[] featureIndices = null;
        DMatrix.RowIterator iter = data.rowIterator();

        for (int epoch = 0; epoch < numIterations; ++epoch) {

            while (iter.hasNextRow()) {

                iter.nextRow();

                if (first) {
                    numFeatures = (int) iter.numCols() - 1;
                    featureIndices = new int[numFeatures];
                    for (int j = 0, k = 0; j < numFeatures + 1; ++j)
                        if (j != labelIndex) {
                            featureIndices[k] = j;
                            ++k;
                        }
                    first = false;
                }

                // -- Compute the Logistic Regression Hypothesis as Sigmoid Function
                int m = 1;
                double p = weights.get(0, 0);
                for (int j : featureIndices) {
                    p += iter.getValueOfColumn(j) * weights.get(0, m);
                    m++;
                }

                // -- Minimize the loss function --
                // Compute parameter Θ(0).
                weights.set(0, 0, weights.get(0, 0) - alpha * lossFunction.dloss(p, iter.getValueOfColumn(labelIndex)));

                // Compute parameters Θ(1..m).
                int n = 1;
                for (int k : featureIndices) {
                    weights.set(0, n, weights.get(0, n) - alpha * lossFunction.dloss(p, iter.getValueOfColumn(labelIndex)) * iter.getValueOfColumn(k));
                    n++;
                }
            }

            if (lossFunction instanceof LogisticLossFunction) {
                if (epoch % 1000 == 0) { // TODO: We have to change that.
                    setLearningRate(getLearningRate() * 3);
                }
            }

            iter.reset();

            if (weightsUpdater != null)
                weightsUpdater.updateWeights(epoch, weights);
        }

        return weights;
    }
}
