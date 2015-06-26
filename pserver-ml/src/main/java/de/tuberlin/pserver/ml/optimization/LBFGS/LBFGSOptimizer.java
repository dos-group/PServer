package de.tuberlin.pserver.ml.optimization.LBFGS;

import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.GradientStepFunction;
import de.tuberlin.pserver.ml.common.LabeledVector;
import de.tuberlin.pserver.ml.optimization.LossFunction;
import de.tuberlin.pserver.ml.optimization.Optimizer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LBFGSOptimizer {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------
/*
    public static abstract class DiffFunction<T> {

        public T gradientAt(final T x) { return calculate(x).getRight(); }

        public double valueAt(final T x) { return calculate(x).getLeft(); }

        public abstract Pair<Double, T> calculate(final T x);
    }

    private static final class State {

        public State(Vector x, Vector grad, ApproximateInverseHessian history) {

            this.x = x;

            this.grad = grad;

            this.history = history;
        }

        public Vector x;

        public Vector grad;

        public ApproximateInverseHessian history;

        public int iter; // TODO: check this!
    }

    // ---------------------------------------------------

    private static final class CostFunction extends DiffFunction<Vector> {


        public final Matrix.RowIterator dataIterator;

        public final LossFunction lossFunction;

        public final GradientStepFunction stepFunction;

        public final double regParam;

        public CostFunction(final Matrix.RowIterator dataIterator,
                            final LossFunction lossFunction,
                            final GradientStepFunction stepFunction,
                            final double regParam) {

            this.dataIterator = dataIterator;
            this.lossFunction = lossFunction;
            this.stepFunction = stepFunction;
            this.regParam = regParam;
        }

        @Override
        public Pair<Double, Vector> calculate(Vector x) {
            final long n = x.size();

            final int numFeatures = (int)dataIterator.numCols() - 1;

            //val bcW = data.context.broadcast(w) ????

            final Vector gradientSum = new DVector(n);

            double lossSum = 0.0;

            while (dataIterator.hasNextRow()) {

                dataIterator.nextRow();

                final double label = dataIterator.getValueOfColumn((int) dataIterator.numCols() - 1);

                final Vector featureVector = dataIterator.getAsVector(0, numFeatures);

                final LabeledVector labeledVector = new LabeledVector(label, featureVector);

                final Pair<Double, Vector> res = lossFunction.lossAndGradient(labeledVector, x);

                lossSum += res.getLeft();

                gradientSum.add(res.getRight());
            }

            double regVal = 0.0; // Regularization...

            double loss = lossSum / dataIterator.numRows() + regVal;

            Vector gradientTotal = new DVector((DVector)x);

            gradientTotal.add(-1.0, stepFunction.takeStep(x, new DVector(n), 1));

            gradientTotal.add(1.0 / dataIterator.numRows(), gradientSum);

            return Pair.of(lossSum, gradientTotal);
        }
    }

    // ---------------------------------------------------

    private static final class ApproximateInverseHessian {

        private List<Vector> memStep = new ArrayList<>();

        private List<Vector> memGradDelta = new ArrayList<>();

        private int m = 10;

        public ApproximateInverseHessian(int m) { this.m = m; }

        public ApproximateInverseHessian(final List<Vector> memStep, final List<Vector> memGradDelta) {

            this.memStep = memStep;

            this.memGradDelta = memGradDelta;
        }

        public ApproximateInverseHessian update(final Vector step , final Vector gradDelta) {

            memStep.add(0, step);

            memGradDelta.add(0, gradDelta);

            return new ApproximateInverseHessian(memStep.subList(0, m), memGradDelta.subList(0, m));
        }

        public Vector mul(final Vector grad) {

            final double diag;

            if (memStep.size() > 0) {

                Vector prevStep = memStep.get(0);

                Vector prevGradStep = memGradDelta.get(0);

                double sy = prevStep.dot(prevGradStep);

                double yy = prevGradStep.dot(prevGradStep);

                diag = sy / yy;

            } else {

                diag = 1.0;
            }

            Vector dir = new DVector((DVector)grad);

            double[] as  = new double[m];

            double[] rho = new double[m];

            for (int i = 0; i < memStep.size(); ++i) {

                rho[i] = memStep.get(i).dot(memGradDelta.get(i));

                as[i] = memStep.get(i).dot(dir) / rho[i];

                if (Double.isNaN(as[i])) {
                    throw new IllegalStateException("NaNHistory");
                }

                dir.add(-as[i], memGradDelta.get(i));
            }

            dir.mul(diag);

            for(int i = (memStep.size() - 1); i > 0; --i) { // TODO: > OR >= ??

                double beta = memGradDelta.get(i).dot(dir) / rho[i];

                dir.add(as[i] - beta, memStep.get(i));
            }

            dir.mul(-1.0);

            return dir;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LBFGSOptimizer.class);

    private int numCorrections = 10;

    private double convergenceTol = 1E-4;

    private int maxNumIterations = 100;

    private double regParam = 0.0;

    private LossFunction lossFunction;

    private GradientStepFunction gradientStepFunction;

    private Vector initialWeights;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LBFGSOptimizer() {

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public GeneralLinearModel optimize(GeneralLinearModel model, Matrix.RowIterator dataIterator) {
        return null;
    }

    @Override
    public Object getState() {
        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    public State iterations(State state, CostFunction f) {

        while (state.iter > 0) { // TODO: Convergence Criteria!

            Vector dir = chooseDescentDirection(state);

            double stepSize = determineStepSize(state, f, dir);

            Vector x = takeStep(state, dir, stepSize);

            Pair<Double, Vector> valueAndGrad = f.calculate(x);

            ApproximateInverseHessian history = updateHistory(x, valueAndGrad.getRight(), state);

            state = new State(x, valueAndGrad.getRight(), history);

            --state.iter;
        }

        return state;
    }

    private Vector chooseDescentDirection(State state) {

        return state.history.mul(state.grad);
    }

    private Vector takeStep(State state, Vector dir, double stepSize) {

        dir.mul(stepSize);

        state.x.add(dir);

        return state.x;
    }

    private ApproximateInverseHessian updateHistory(Vector newX, Vector newGrad, State oldState) {

        Vector nx = new DVector((DVector)newX);

        nx.sub(oldState.x);

        Vector ng = new DVector((DVector)newGrad);

        ng.sub(oldState.grad);

        oldState.history.update(nx, ng);

        return oldState.history;
    }

    private double determineStepSize(State state, CostFunction f, Vector dir) {

        Vector x = state.x;

        Vector grad = state.grad;

        DiffFunction<Double> ff = LineSearch.functionFromSearchDirection(f, x, dir);

        StrongWolfeLineSearch search = new StrongWolfeLineSearch(10, 10); // TODO: Need good default values here.

        double init = 1.0;

        if (state.iter == 0.0)
            init = 1.0 / dir.norm(1.0);

        double alpha = search.minimize(ff, init);

        if (alpha * grad.norm(1.0) < 1E-10)
            throw new IllegalStateException("StepSizeUnderflow");

        return alpha;
    }

    // ---------------------------------------------------
    // LINE SEARCH.
    // ---------------------------------------------------

    public static final class LineSearch {

        public static DiffFunction<Double> functionFromSearchDirection(final DiffFunction<Vector> f, final Vector x, final Vector dir) {

            return new DiffFunction<Double>() {

                @Override
                public Double gradientAt(final Double alpha) {
                    Vector r_dir = new DVector((DVector)dir);
                    r_dir.mul(alpha);
                    Vector r_x = new DVector((DVector)x);
                    r_x.add(r_dir);
                    Vector g_x = f.gradientAt(r_x);
                    return g_x.dot(dir);
                }

                @Override
                public double valueAt(final Double alpha) {
                    Vector r_dir = new DVector((DVector)dir);
                    r_dir.mul(alpha);
                    Vector r_x = new DVector((DVector)x);
                    r_x.add(r_dir);
                    return f.valueAt(r_x);
                }

                @Override
                public Pair<Double, Double> calculate(Double alpha) {
                    Vector r_dir = new DVector((DVector)dir);
                    r_dir.mul(alpha);
                    Vector r_x = new DVector((DVector)x);
                    r_x.add(r_dir);
                    Pair<Double,Vector> r = f.calculate(r_x);
                    return Pair.of(r.getLeft(), r.getRight().dot(dir));
                }
            };
        }
    }

    public static abstract class CubicLineSearch {

        public static class Bracket {

            public double t;

            public double dd;

            public double fval;
        }

        public abstract double minimize(final DiffFunction<Double> f, final double init);

        public double interp(Bracket l, Bracket r) {

            double d1 = l.dd + r.dd - 3 * (l.fval - r.fval) / (l.t - r.t);

            double d2 = Math.sqrt(d1 * d1 - l.dd * r.dd);

            double multiplier = r.t - l.t;

            double t = r.t - multiplier * (r.dd + d2 - d1) / (r.dd - l.dd + 2 * d2);

            double lbound = l.t + 0.1 * (r.t - l.t);

            double ubound = l.t + 0.9 * (r.t - l.t);

            if (t < lbound) return lbound;
            if (t > ubound) return ubound;
            return t;
        }
    }

    public class StrongWolfeLineSearch extends CubicLineSearch {

        public double c1 = 1e-4;

        public double c2 = 0.9;

        public int maxZoomIter;

        public int maxLineSearchIter;

        public StrongWolfeLineSearch(int maxZoomIter, int maxLineSearchIter) {

            this.maxZoomIter = maxZoomIter;

            this.maxLineSearchIter = maxLineSearchIter;
        }

        @Override
        public double minimize(final DiffFunction<Double> f, final double init) {

            double t = init;

            Bracket low = phi(0.0, f);

            double fval = low.fval;

            double dd = low.dd;

            if (dd > 0) {
                throw new IllegalStateException("Line search invoked with non-descent direction: " + dd);
            }

            for (int i = 0; i < maxLineSearchIter; ++i) {

                Bracket c = phi(t, f);

                if (Double.isInfinite(c.fval) || Double.isNaN(c.fval)) {

                    t /= 2.0;

                } else {

                    if ((c.fval > fval + c1 * t * dd) ||
                            (c.fval >= low.fval && i > 0)) {
                        return zoom(low, c, f, dd, fval);
                    }

                    if (Math.abs(c.dd) <= c2 * Math.abs(dd)) {
                        return c.t;
                    }

                    if (c.dd >= 0) {
                        return zoom(c, low, f, dd, fval);
                    }

                    low = c;
                    t *= 1.5;
                }
            }

            throw new IllegalStateException("Line search failed");
        }

        private Bracket phi(double t, DiffFunction<Double> f) {

            Pair<Double, Double> p = f.calculate(t);

            Bracket b = new Bracket();

            b.t  = t;

            b.dd = p.getRight();

            b.fval = p.getLeft();

            return b;
        }

        private double zoom(Bracket linit, Bracket rinit, DiffFunction<Double> f, double dd, double fval) {

            Bracket low = linit;
            Bracket hi = rinit;

            for (int i = 0; i < maxZoomIter; ++i) {

                double t = 0.0;

                if (low.t > hi.t)
                    t = interp(hi, low);
                else
                    t = interp(low, hi);

                Bracket c = phi(t, f);

                if (c.fval > fval + c1 * c.t * dd || c.fval >= low.fval) {

                    hi = c;

                } else {

                    if (Math.abs(c.dd) <= c2 * Math.abs(dd)) {
                        return c.t;
                    }

                    if (c.dd * (hi.t - low.t) >= 0) {
                        hi = low;
                    }

                    low = c;
                }
            }

            throw new IllegalStateException("Line search zoom failed");
        }
    }*/
}
