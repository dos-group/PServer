package de.tuberlin.pserver.ml.optimization;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;

public interface ScoreFunction {

    double score(final Matrix32F yTrue, final Matrix32F yPred);

    public class ZeroOneLoss implements ScoreFunction {

        @Override
        public double score(final Matrix32F yTrue, final Matrix32F yPred) {
            Preconditions.checkArgument(yTrue.rows() == yPred.rows());
            double loss = 0.0;
            for (int i = 0; i < yTrue.rows(); ++i) {
                if (yTrue.get(i) != yPred.get(i)) {
                    loss++;
                }
            }
            return loss;
        }
    }

    public class Accuracy implements ScoreFunction {

        @Override
        public double score(final Matrix32F yTrue, final Matrix32F yPred) {
            Preconditions.checkArgument(yTrue.rows() == yPred.rows());
            double loss = new ZeroOneLoss().score(yTrue, yPred);
            return (yTrue.rows() - loss) / yTrue.rows();
        }
    }
}
