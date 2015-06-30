package de.tuberlin.pserver.ml.playground.mahout;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.stuff.DoubleFunction;
import de.tuberlin.pserver.math.stuff.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Generic definition of a 1 of n logistic regression classifier that returns probabilities in
 * response to a feature vector.  This classifier uses 1 of n-1 coding where the 0-th category
 * is not stored explicitly.
 * <p/>
 * Provides the SGD based algorithm for learning a logistic regression, but omits all
 * annealing of learning rates.  Any extension of this abstract class must define the overall
 * and per-term annealing for themselves.
 */
public abstract class AbstractOnlineLogisticRegression extends AbstractVectorClassifier implements OnlineLearner {


    private static final Logger LOG = LoggerFactory.getLogger(AbstractOnlineLogisticRegression.class);

    // coefficients for the classification.  This is a dense matrix
    // that is (numCategories-1) x numFeatures
    protected Matrix beta; // THE MODEL!!!!

    // number of categories we are classifying.  This should the number of rows of beta plus one.
    protected int numCategories;

    protected int step;

    // information about how long since coefficient rows were updated.  This allows lazy regularization.
    protected Vector updateSteps;

    // information about how many updates we have had on a location.  This allows per-term
    // annealing a la confidence weighted learning.
    protected Vector updateCounts;

    // weight of the prior on beta
    private double lambda = 1.0e-5;
    protected PriorFunction prior;

    // can we ignore any further regularization when doing classification?
    private boolean sealed;

    // by default we don't do any fancy training
    private Gradient gradient = new DefaultGradient();

    /**
     * Chainable configuration option.
     *
     * @param lambda New value of lambda, the weighting factor for the prior distribution.
     * @return This, so other configurations can be chained.
     */
    public AbstractOnlineLogisticRegression lambda(double lambda) {
        this.lambda = lambda;
        return this;
    }

    /**
     * Computes the inverse link function, by default the logistic link function.
     *
     * @param v The output of the linear combination in a GLM.  Note that the value
     *          of v is disturbed.
     * @return A version of v with the link function applied.
     */
    public Vector link(Vector v) {
        double max = v.maxValue();
        if (max >= 40) {
            // if max > 40, we subtract the large offset first
            // the length of the max means that 1+sum(exp(v)) = sum(exp(v)) to within round-off
            v.assign(Functions.minus(max)).assign(Functions.EXP);
            return v.div(v.norm(1));
        } else {
            v.assign(Functions.EXP);
            return v.div(1 + v.norm(1));
        }
    }

    /**
     * Computes the binomial logistic inverse link function.
     *
     * @param r The value to transform.
     * @return The logit of r.
     */
    public double link(double r) {
        if (r < 0.0) {
            double s = Math.exp(r);
            return s / (1.0 + s);
        } else {
            double s = Math.exp(-r);
            return 1.0 / (1.0 + s);
        }
    }

    @Override
    public Vector classifyNoLink(Vector instance) {
        // apply pending regularization to whichever coefficients matter

        regularize(instance);
        return beta.mul(instance);
    }

    public double classifyScalarNoLink(Vector instance) {
        return beta.rowAsVector(0).dot(instance);
    }

    /**
     * Returns n-1 probabilities, one for each category but the 0-th.  The probability of the 0-th
     * category is 1 - sum(this result).
     *
     * @param instance A vector of features to be classified.
     * @return A vector of probabilities, one for each of the first n-1 categories.
     */
    @Override
    public Vector classify(Vector instance) {
        return link(classifyNoLink(instance));
    }

    /**
     * Returns a single scalar probability in the case where we have two categories.  Using this
     * method avoids an extra vector allocation as opposed to calling classify() or an extra two
     * vector allocations relative to classifyFull().
     *
     * @param instance The vector of features to be classified.
     * @return The probability of the first of two categories.
     * @throws IllegalArgumentException If the classifier doesn't have two categories.
     */
    @Override
    public double classifyScalar(Vector instance) {
        Preconditions.checkArgument(numCategories() == 2, "Can only call classifyScalar with two categories");

        // apply pending regularization to whichever coefficients matter
        regularize(instance);

        // result is a vector with one element so we can just use dot product
        return link(classifyScalarNoLink(instance));
    }

    @Override
    public void train(long trackingKey, String groupKey, int actual, Vector instance) {
        unseal();

        double learningRate = currentLearningRate();

        // push coefficients back to zero based on the prior
        regularize(instance);

        // update each row of coefficients according to result
        Vector gradient = this.gradient.apply(groupKey, actual, instance, this);
        for (int i = 0; i < numCategories - 1; i++) {
            double gradientBase = gradient.get(i);

            // then we apply the gradientBase to the resulting element.
            Iterator<Vector.Element> nonZeros = instance.iterateNonZero();
            while (nonZeros.hasNext()) {
                Vector.Element updateLocation = nonZeros.next();
                int j = updateLocation.index();

                double newValue = beta.get(i, j) + gradientBase * learningRate * perTermLearningRate(j) * instance.get(j);
                beta.set(i, j, newValue);
            }
        }

        // remember that these elements got updated
        Iterator<Vector.Element> i = instance.iterateNonZero();
        while (i.hasNext()) {
            Vector.Element element = i.next();
            int j = element.index();
            updateSteps.set(j, getStep());
            updateCounts.set(j, updateCounts.get(j) + 1);
        }
        nextStep();
    }

    @Override
    public void train(long trackingKey, int actual, Vector instance) {
        train(trackingKey, null, actual, instance);
    }

    @Override
    public void train(int actual, Vector instance) {
        train(0, null, actual, instance);
    }

    public void regularize(Vector instance) {
        if (updateSteps == null || isSealed()) {
            return;
        }

        // anneal learning rate
        double learningRate = currentLearningRate();

        // here we lazily apply the prior to make up for our neglect
        for (int i = 0; i < numCategories - 1; i++) {
            Iterator<Vector.Element> nonZeros = instance.iterateNonZero();
            while (nonZeros.hasNext()) {
                Vector.Element updateLocation = nonZeros.next();
                int j = updateLocation.index();
                double missingUpdates = getStep() - updateSteps.get(j);
                if (missingUpdates > 0) {
                    double rate = getLambda() * learningRate * perTermLearningRate(j);
                    double newValue = prior.age(beta.get(i, j), missingUpdates, rate);
                    beta.set(i, j, newValue);
                    updateSteps.set(j, getStep());
                }
            }
        }
    }

    // these two abstract methods are how extensions can modify the basic learning behavior of this object.

    public abstract double perTermLearningRate(int j);

    public abstract double currentLearningRate();

    public void setPrior(PriorFunction prior) { this.prior = prior; }

    public void setGradient(Gradient gradient) { this.gradient = gradient; }

    public PriorFunction getPrior() { return prior; }

    public Matrix getBeta() { close(); return beta; }

    public void setBeta(int i, int j, double betaIJ) { beta.set(i, j, betaIJ); }

    @Override
    public int numCategories() { return numCategories; }

    public int numFeatures() { return (int)beta.numCols(); }

    public double getLambda() { return lambda; }

    public int getStep() { return step; }

    protected void nextStep() { step++; }

    public boolean isSealed() { return sealed; }

    protected void unseal() { sealed = false; }

    private void regularizeAll() {
        Vector all = new DVector(beta.numCols());
        all.assign(1);
        regularize(all);
    }

    @Override
    public void close() {
        if (!sealed) {
            step++;
            regularizeAll();
            sealed = true;
        }
    }

    public void copyFrom(AbstractOnlineLogisticRegression other) {
        // number of categories we are classifying.  This should the number of rows of beta plus one.
        Preconditions.checkArgument(numCategories == other.numCategories,
                "Can't copy unless number of target categories is the same");

        beta.assign(other.beta);

        step = other.step;

        updateSteps.assign(other.updateSteps);
        updateCounts.assign(other.updateCounts);
    }

    public boolean validModel() {
        double k = beta.aggregate(Functions.PLUS, new DoubleFunction() {
            @Override
            public double apply(double v) {
                return Double.isNaN(v) || Double.isInfinite(v) ? 1 : 0;
            }
        });
        return k < 1;
    }
}