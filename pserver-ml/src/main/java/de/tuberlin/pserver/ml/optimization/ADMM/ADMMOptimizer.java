package de.tuberlin.pserver.ml.optimization.ADMM;


import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.ml.models.GeneralLinearModel;
import de.tuberlin.pserver.ml.optimization.Optimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADMMOptimizer implements Optimizer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(ADMMOptimizer.class);

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ADMMOptimizer() {
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public GeneralLinearModel optimize(GeneralLinearModel model, Matrix.RowIterator dataIterator) {
        return null;
    }

    @Override
    public Matrix optimize(GeneralLinearModel model, Matrix example) {
        return null;
    }
}
