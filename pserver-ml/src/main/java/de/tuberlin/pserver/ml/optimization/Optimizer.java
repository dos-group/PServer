package de.tuberlin.pserver.ml.optimization;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;

public interface Optimizer {

    public abstract GeneralLinearModel optimize(final GeneralLinearModel model, final Matrix.RowIterator dataIterator);

    public abstract Matrix optimize(final GeneralLinearModel model, final Matrix example);
}
