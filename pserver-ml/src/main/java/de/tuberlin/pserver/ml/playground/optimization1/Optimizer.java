package de.tuberlin.pserver.ml.playground.optimization1;


import de.tuberlin.pserver.app.Stateful;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;

public interface Optimizer extends Stateful {

    public abstract GeneralLinearModel optimize(final GeneralLinearModel model, final Matrix.RowIterator dataIterator);
}
