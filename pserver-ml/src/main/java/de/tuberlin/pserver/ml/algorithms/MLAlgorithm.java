package de.tuberlin.pserver.ml.algorithms;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.app.Stateful;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.ml.models.Model;

public abstract class MLAlgorithm<T extends Model> implements Stateful {

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
