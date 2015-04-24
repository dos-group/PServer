package de.tuberlin.pserver.ml.optimization.gradientdescent;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerContext;
import de.tuberlin.pserver.app.types.DMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGDRegressor implements SGDBase {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(SGDRegressor.class);

    protected LossFunction lossFunction;

    protected double alpha;

    protected int numIterations;

    protected int instanceID;

    protected WeightsUpdater weightsUpdater;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SGDRegressor(final PServerContext ctx) { this(Preconditions.checkNotNull(ctx).instanceID); }
    public SGDRegressor(final int instanceID) {
        this.lossFunction = new SquaredLossFunction();
        setInstanceID(instanceID);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public SGDRegressor setLossFunction(final LossFunction lossFunction) { this.lossFunction = lossFunction; return this; }

    public SGDRegressor setLearningRate(final double alpha) { this.alpha = alpha; return this; }

    public SGDRegressor setNumberOfIterations(final int numIterations) { this.numIterations = numIterations; return this; }

    public SGDRegressor setInstanceID(final int instanceID) { this.instanceID = instanceID; return this; }

    public SGDRegressor setWeightsUpdater(final WeightsUpdater updater) { this.weightsUpdater = updater; return this; }

    // ---------------------------------------------------

    public LossFunction getLossFunction() { return lossFunction; }

    public double getLearningRate() { return alpha; }

    public int getNumIterations() { return numIterations; }

    public int getInstanceID() { return instanceID; }

    public WeightsUpdater getWeightsUpdater() { return weightsUpdater; }

    // ---------------------------------------------------

    @Override
    public DMatrix predict(final DMatrix weights, final DMatrix data) { return null; }

    @Override
    public DMatrix fit(final DMatrix weights, final DMatrix data, final int labelIndex) {
        return doPlainSGD(
                Preconditions.checkNotNull(weights),
                Preconditions.checkNotNull(data),
                labelIndex,
                lossFunction,
                alpha,
                numIterations
        );
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private DMatrix doPlainSGD(final DMatrix weights,
                               final DMatrix data,
                               final int labelIndex,
                               final LossFunction lossFunction,
                               final double alpha,
                               final int numIterations) {

        boolean first           = true;
        int numFeatures         = 0;
        int[] featureIndices    = null;
        DMatrix.RowIterator iter = data.rowIterator();

        if (lossFunction instanceof SquaredLossFunction) {
            for (int epoch = 0; epoch < numIterations; ++epoch) {

                while(iter.hasNextRow()) {

                    iter.nextRow();

                    if (first) {
                        numFeatures     = (int)iter.numCols() - 1;
                        featureIndices  = new int[numFeatures];
                        for (int j = 0, k = 0; j < numFeatures + 1; ++j)
                            if (j != labelIndex) {
                                featureIndices[k] = j;
                                ++k;
                            }
                        first = false;
                    }

                    // -- Compute the prediction (p) --
                    // Dot product of a sample x and the weight vector.
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

                iter.reset();

                if (weightsUpdater != null)
                    weightsUpdater.updateWeights(epoch, weights);
            }
        } else
            throw new IllegalStateException();

        return weights;
    }
}
