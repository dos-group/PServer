package de.tuberlin.pserver.math.generators;

import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.dense.DVector;

import java.util.Random;

public class VectorGenerator {

    private static final Random rand = new Random();

    public static Vector RandomDVector(long size) {
        double[] data = BufferGenerator.RandomValues(size);
        return new DVector(size, data, Layout.COLUMN_LAYOUT);
    }

    public static Vector RandomDVector(long size, Layout type) {
        double[] data = BufferGenerator.RandomValues(size);
        return new DVector(size, data, type);
    }

    public static Vector RandomSVector(long size) {
        return RandomSVector(size, 0.9);
    }

    public static Vector RandomSVector(long size, double sparsity) {
        return RandomSVector(size, Layout.COLUMN_LAYOUT, sparsity);
    }

    public static Vector RandomSVector(long size, Layout type) {
        return RandomSVector(size, type, 0.9);
    }

    public static Vector RandomSVector(long size, Layout type, double sparsity) {
        double[] data = BufferGenerator.SparseRandomValues(size, sparsity);
        return new DVector(size, data, type);
    }


}
