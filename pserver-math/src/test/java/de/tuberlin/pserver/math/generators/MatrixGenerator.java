package de.tuberlin.pserver.math.generators;

import de.tuberlin.pserver.math.DMatrix;
import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.Vector;

import java.util.Random;

public class MatrixGenerator {

    private static final Random rand = new Random();

    public static Matrix RandomDMatrix(long rows, long cols) {
        double[] data = new double[(int) (rows*cols)];
        for(int i=0; i<data.length; i++) {
            data[i] = rand.nextDouble();
        }
        return new DMatrix(rows, cols, data, DMatrix.MemoryLayout.ROW_LAYOUT);
    }

    public static Matrix RandomDMatrix(long rows, long cols, DMatrix.MemoryLayout layout) {
        double[] data = new double[(int) (rows*cols)];
        for(int i=0; i<data.length; i++) {
            data[i] = rand.nextDouble();
        }
        return new DMatrix(rows, cols, data, layout);
    }

}
