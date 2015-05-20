package de.tuberlin.pserver.ml.playground.mahout.test;

import de.tuberlin.pserver.math.DMatrix;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

public class TestMahoutLogisticRegression {

    public static void main(String[] args) {

        Matrix m = new DMatrix(1, 100);
        Vector v1 = new DVector(100, Vector.VectorType.COLUMN_VECTOR);

        Vector v2 = m.mul(v1);

    }
}
