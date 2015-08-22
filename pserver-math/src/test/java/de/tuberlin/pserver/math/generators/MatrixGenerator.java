package de.tuberlin.pserver.math.generators;

import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;

import java.util.Random;

public class MatrixGenerator {

    private static final Random rand = new Random();

    public static DMatrix RandomDMatrix(long rows, long cols) {
        double[] data = BufferGenerator.RandomValues(rows, cols);
        return new DMatrix(rows, cols, data, Layout.ROW_LAYOUT);
    }

    public static DMatrix RandomDMatrix(long rows, long cols, Layout layout) {
        double[] data = BufferGenerator.RandomValues(rows, cols);
        return new DMatrix(rows, cols, data, layout);
    }

    public static DMatrix AscendingDMatrix(long rows, long cols) {
        double[] data = BufferGenerator.AscendingValues(rows, cols);
        return new DMatrix(rows, cols, data, Layout.ROW_LAYOUT);
    }

    public static DMatrix AscendingDMatrix(long rows, long cols, Layout layout) {
        double[] data = BufferGenerator.AscendingValues(rows, cols);
        return new DMatrix(rows, cols, data, layout);
    }

}
