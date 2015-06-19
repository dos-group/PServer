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

    public static Vector RandomSVector(long size) {
        return RandomSVector(size, 0.9);
    }

    public static Vector RandomSVector(long size, double sparsity) {
        return RandomSVector(size, Vector.VectorType.COLUMN_VECTOR, sparsity);
    }

    public static Vector RandomSVector(long size, Vector.VectorType type) {
        return RandomSVector(size, type, 0.9);
    }

    public static Vector RandomSVector(long size, Vector.VectorType type, double sparsity) {
        double[] data = BufferGenerator.SparseRandomValues(size, sparsity);
        return new DVector(size, data, type);
    }


}
