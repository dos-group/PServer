package de.tuberlin.pserver.ml.optimization;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;


public class Scorer {

    private ScoreFunction scoreFunction;
    private PredictionFunction predictionFunction;

    public Scorer(ScoreFunction scoreFunction, PredictionFunction predictionFunction) {
        this.scoreFunction = Preconditions.checkNotNull(scoreFunction);
        this.predictionFunction = Preconditions.checkNotNull(predictionFunction);
    }

    @SuppressWarnings("unchecked")
    public double score(Matrix32F X, Matrix32F y, Matrix32F W) throws Exception {
        Matrix32F yPred = new MatrixBuilder().dimension(y.rows(), y.cols()).build();

        for (int i = 0; i < y.rows(); ++i) {
            yPred.set(i, 0, predictionFunction.predict(X.getRow(i), W));
        }

        return scoreFunction.score(y, yPred);
    }
}
