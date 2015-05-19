package de.tuberlin.pserver.ml.optimization.gradientdescent;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerContext;
import de.tuberlin.pserver.experimental.old.DMatrix;

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
    public DMatrix predict(final DMatrix weights, final DMatrix data) {
        throw new UnsupportedOperationException();
    }

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
}
