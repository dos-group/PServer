package de.tuberlin.pserver.ml.models;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.runtime.SlotContext;

public class GeneralLinearModel extends Model<GeneralLinearModel> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long length;

    private Matrix weights;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GeneralLinearModel(final String name, final long length) {
        this(name, 0, length, null);
    }

    public GeneralLinearModel(final GeneralLinearModel lm) {
        this(Preconditions.checkNotNull(lm.name), lm.nodeID, lm.length, Preconditions.checkNotNull(lm.weights).copy());
    }

    public GeneralLinearModel(final String name, final int nodeID, final long length, final Matrix weights) {
        super(name, nodeID);
        this.length     = length;
        this.weights    = weights;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void createModel(final SlotContext ctx) {
        Preconditions.checkNotNull(ctx);
        Preconditions.checkArgument(length > 0);

        Matrix weights = new MatrixBuilder()
                .dimension(length, 1)
                .format(Format.DENSE_FORMAT)
                .layout(Layout.COLUMN_LAYOUT)
                .build();

        ctx.runtimeContext.dataManager.putObject(name, weights);
    }

    @Override
    public void fetchModel(final SlotContext ctx) {
        Preconditions.checkNotNull(ctx);
        weights = ctx.runtimeContext.dataManager.getObject(name);
    }

    @Override
    public GeneralLinearModel copy() { return new GeneralLinearModel(this); }

    @Override
    public String toString() { return "\nLinearModel " + gson.toJson(this); }

    // ---------------------------------------------------

    public Matrix getWeights() { return weights; }

    public void updateModel(final Matrix update) { weights.assign(update); }
}
