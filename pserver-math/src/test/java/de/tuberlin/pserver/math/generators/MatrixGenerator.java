package de.tuberlin.pserver.math.generators;

import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.dense.Dense64Matrix;

import java.util.Random;

public class MatrixGenerator {

    private static final Random rand = new Random();

    public static Dense64Matrix RandomDMatrix(long rows, long cols) {
        double[] data = BufferGenerator.RandomValues(rows, cols);
        return new Dense64Matrix(rows, cols, data, Layout.ROW_LAYOUT);
    }

    public static Dense64Matrix RandomDMatrix(long rows, long cols, Layout layout) {
        double[] data = BufferGenerator.RandomValues(rows, cols);
        return new Dense64Matrix(rows, cols, data, layout);
    }

    public static Dense64Matrix AscendingDMatrix(long rows, long cols) {
        double[] data = BufferGenerator.AscendingValues(rows, cols);
        return new Dense64Matrix(rows, cols, data, Layout.ROW_LAYOUT);
    }

    public static Dense64Matrix AscendingDMatrix(long rows, long cols, Layout layout) {
        double[] data = BufferGenerator.AscendingValues(rows, cols);
        return new Dense64Matrix(rows, cols, data, layout);
    }

}
