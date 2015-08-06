package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.core.common.Registerable;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;

public interface Optimizer extends Registerable {

    public abstract GeneralLinearModel optimize(final GeneralLinearModel model, final Matrix.RowIterator dataIterator);

    public abstract Vector optimize(final GeneralLinearModel model, final Vector example);
}
