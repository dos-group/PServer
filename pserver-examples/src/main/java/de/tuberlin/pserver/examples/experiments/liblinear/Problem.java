package de.tuberlin.pserver.examples.experiments.liblinear;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.io.Serializable;


public class Problem implements Serializable {

    public final Matrix dataPoints;

    public final long n;

    public final long l;

    public double bias = -1.0;

    public Problem(final Matrix dataPoints) {

        this.dataPoints = Preconditions.checkNotNull(dataPoints);

        this.l = dataPoints.rows();

        this.n = (bias >= 0) ? dataPoints.cols() + 2 : dataPoints.cols() + 1;
    }

    public Problem genBinaryProblem(final double posLabel) {

        // ??

        return null;
    }

    /*def genBinaryProb(posLabel : Double) : Problem = {
        var binaryProb = new Problem()
        binaryProb.l = this.l
        binaryProb.n = this.n
        binaryProb.bias = this.bias
        // Compute Problem Label
        val dataPoints = this.dataPoints.mapPartitions(blocks => {
                blocks.map(p => p.genTrainingPoint(this.n, this.bias, posLabel))
        }).cache()
        binaryProb.dataPoints = dataPoints
        binaryProb
    }*/
}
