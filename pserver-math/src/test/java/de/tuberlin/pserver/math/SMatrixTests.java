package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.generators.BufferGenerator;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.sparse.SMatrix;
import org.junit.Test;

import java.util.Random;

public class SMatrixTests {

    @Test
    public void testConversionDMatrixToSMatrix() {
        Random random = new Random(42656234756L);
        int rows, cols;
        for (int i = 0; i < 1000; i++) {
            rows = 1 + random.nextInt(500);
            cols = 1 + random.nextInt(500);
            double[] data = BufferGenerator.SparseRandomValues(rows, cols, 0.9);
            for (Matrix.Layout srcLayout : Matrix.Layout.values()) {
                for (Matrix.Layout targetLayout : Matrix.Layout.values()) {
                    DMatrix dMat = new DMatrix(rows, cols, data, srcLayout);
                    SMatrix sMat = SMatrix.fromDMatrix(dMat, targetLayout);
                    TestUtils.checkValues(dMat, sMat);
                }
            }
        }
    }

    @Test
    public void testConversionSMatrixToDMatrix() {
        /*Random random = new Random(42656234756L);
        int rows, cols;
        for (int i = 0; i < 1000; i++) {
            rows = 1 + random.nextInt(500);
            cols = 1 + random.nextInt(500);
            double[] data = BufferGenerator.SparseRandomValues(rows, cols, 0.9);
            for (Matrix.MemoryLayout srcLayout : Matrix.MemoryLayout.values()) {
                for (Matrix.MemoryLayout targetLayout : Matrix.MemoryLayout.values()) {
                    SMatrix sMat = SMatrix.fromDMatrix(new DMatrix(rows, cols, data, srcLayout), srcLayout);
                    DMatrix dMat = DMatrix.fromSMatrix(sMat, targetLayout);
                    TestUtils.checkValues(dMat, sMat);
                }
            }
        }*/
    }
}
