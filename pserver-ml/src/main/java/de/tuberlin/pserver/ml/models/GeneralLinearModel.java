package de.tuberlin.pserver.ml.models;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.PServerContext;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

public class GeneralLinearModel extends Model<GeneralLinearModel> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int size;

    private Vector weights;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GeneralLinearModel(final String name, final int size) {
        this(name, 0, size, null);
    }

    public GeneralLinearModel(final GeneralLinearModel lm) {
        this(Preconditions.checkNotNull(lm.name), lm.instanceID, lm.size, Preconditions.checkNotNull(lm.weights).copy());
    }

    public GeneralLinearModel(final String name, final int instanceID, final int size, final Vector weights) {
        super(name, instanceID);
        this.size       = size;
        this.weights    = weights;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void createModel(final PServerContext ctx) {
        Preconditions.checkArgument(size > 0);
        weights = Preconditions.checkNotNull(ctx).dataManager.createLocalVector(name, size, Vector.VectorType.COLUMN_VECTOR);
    }

    @Override
    public void fetchModel(final PServerContext ctx) {
        weights = Preconditions.checkNotNull(ctx).dataManager.getLocalVector(name);
    }

    @Override
    public GeneralLinearModel copy() { return new GeneralLinearModel(this); }

    @Override
    public String toString() { return "\nLinearModel " + gson.toJson(this); }

    // ---------------------------------------------------

    public Vector getWeights() { return weights; }

    public void updateModel(final Vector update) { weights.assign(update); }
}
