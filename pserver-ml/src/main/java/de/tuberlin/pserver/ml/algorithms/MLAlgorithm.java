package de.tuberlin.pserver.ml.algorithms;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.common.Registerable;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.ml.models.Model;
import de.tuberlin.pserver.runtime.InstanceContext;

public abstract class MLAlgorithm<T extends Model> implements Registerable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final InstanceContext ctx;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MLAlgorithm(final InstanceContext ctx) {
        this.ctx = Preconditions.checkNotNull(ctx);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void train(final T model, final Matrix data);
}
