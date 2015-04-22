package de.tuberlin.pserver.ml.optimization.gradientdescent;

import de.tuberlin.pserver.app.types.DMatrix;

public interface SGDBase {

    // ---------------------------------------------------
    // Abstract Methods.
    // ---------------------------------------------------

    public abstract DMatrix predict(final DMatrix weights, final DMatrix data);

    public abstract DMatrix fit(final DMatrix weights, final DMatrix data, final int labelIndex);
}
