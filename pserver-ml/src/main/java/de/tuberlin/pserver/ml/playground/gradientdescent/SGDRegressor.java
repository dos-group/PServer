package de.tuberlin.pserver.ml.playground.gradientdescent;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerContext;
import de.tuberlin.pserver.math.Matrix;

public class SGDRegressor extends SGDBase {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SGDRegressor(final PServerContext ctx) {
        super(ctx);
        setLossFunction(new SquaredLossFunction());
    }

    // ---------------------------------------------------

    @Override
    public Matrix predict(final Matrix weights, final Matrix data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Matrix fit(final Matrix weights, final Matrix data, final int labelIndex) {
        return doPlainSGD(
                Preconditions.checkNotNull(weights),
                Preconditions.checkNotNull(data),
                labelIndex,
                lossFunction,
                alpha,
                numIterations
        );
    }
}
