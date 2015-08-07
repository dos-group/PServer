package de.tuberlin.pserver.math.generators;

import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.sparse.SMatrix;

import java.util.Random;

public class MatrixGenerator {

    private static final Random rand = new Random();

    public static DMatrix RandomDMatrix(long rows, long cols) {
        double[] data = BufferGenerator.RandomValues(rows, cols);
        return new DMatrix(rows, cols, data, Matrix.Layout.ROW_LAYOUT);
    }

    public static DMatrix RandomDMatrix(long rows, long cols, Matrix.Layout layout) {
        double[] data = BufferGenerator.RandomValues(rows, cols);
        return new DMatrix(rows, cols, data, layout);
    }

    public static SMatrix RandomSMatrix(long rows, long cols) {
        return RandomSMatrix(rows, cols, Matrix.Layout.ROW_LAYOUT);
    }

    public static SMatrix RandomSMatrix(long rows, long cols, Matrix.Layout layout) {
        double[] data = BufferGenerator.SparseRandomValues(rows, cols);
        return SMatrix.fromDMatrix(new DMatrix(rows, cols, data, layout));
    }

    public static SMatrix RandomSMatrix(long rows, long cols, Matrix.Layout layout, double sparsity) {
        double[] data = BufferGenerator.SparseRandomValues(rows, cols, sparsity);
        return SMatrix.fromDMatrix(new DMatrix(rows, cols, data, layout));
    }

    public static DMatrix AscendingDMatrix(long rows, long cols) {
        double[] data = BufferGenerator.AscendingValues(rows, cols);
        return new DMatrix(rows, cols, data, Matrix.Layout.ROW_LAYOUT);
    }

    public static DMatrix AscendingDMatrix(long rows, long cols, Matrix.Layout layout) {
        double[] data = BufferGenerator.AscendingValues(rows, cols);
        return new DMatrix(rows, cols, data, layout);
    }

}
