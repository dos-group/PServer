package de.tuberlin.pserver.math.generators;

import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Vector;

import java.util.Random;

public class VectorGenerator {

    private static final Random rand = new Random();

    public static Vector RandomDVector(long size) {
        double[] data = BufferGenerator.RandomValues(size);
        return new DVector(size, data, Vector.VectorType.COLUMN_VECTOR);
    }

    public static Vector RandomDVector(long size, Vector.VectorType type) {
        double[] data = BufferGenerator.RandomValues(size);
        return new DVector(size, data, type);
    }

}
