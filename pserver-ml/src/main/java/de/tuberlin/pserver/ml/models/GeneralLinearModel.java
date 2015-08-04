package de.tuberlin.pserver.ml.models;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.InstanceContext;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.math.VectorBuilder;

public class GeneralLinearModel extends Model<GeneralLinearModel> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long length;

    private Vector weights;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GeneralLinearModel(final String name, final long length) {
        this(name, 0, length, null);
    }

    public GeneralLinearModel(final GeneralLinearModel lm) {
        this(Preconditions.checkNotNull(lm.name), lm.nodeID, lm.length, Preconditions.checkNotNull(lm.weights).copy());
    }

    public GeneralLinearModel(final String name, final int nodeID, final long length, final Vector weights) {
        super(name, nodeID);
        this.length     = length;
        this.weights    = weights;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void createModel(final InstanceContext ctx) {
        Preconditions.checkNotNull(ctx);
        Preconditions.checkArgument(length > 0);

        Vector weights = new VectorBuilder()
                .dimension(length)
                .format(Vector.Format.DENSE_VECTOR)
                .layout(Vector.Layout.COLUMN_LAYOUT)
                .build();

        ctx.jobContext.dataManager.putObject(name, weights);
    }

    @Override
    public void fetchModel(final InstanceContext ctx) {
        Preconditions.checkNotNull(ctx);
        weights = ctx.jobContext.dataManager.getObject(name);
    }

    @Override
    public GeneralLinearModel copy() { return new GeneralLinearModel(this); }

    @Override
    public String toString() { return "\nLinearModel " + gson.toJson(this); }

    // ---------------------------------------------------

    public Vector getWeights() { return weights; }

    public void updateModel(final Vector update) { weights.assign(update); }
}
