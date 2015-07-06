package de.tuberlin.pserver.ml.optimization;


import de.tuberlin.pserver.app.Stateful;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;

public interface Optimizer extends Stateful {

    public abstract GeneralLinearModel optimize(final GeneralLinearModel model, final Matrix.RowIterator dataIterator);

    public abstract Vector optimize(final GeneralLinearModel model, final Vector example);
}
