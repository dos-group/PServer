package de.tuberlin.pserver.math.generators;

import java.util.Random;

/**
 * Created by fsander on 01.06.15.
 */
public class BufferGenerator {

    private static final Random rand = new Random();

    public static double[] RandomValues(long rows, long cols) {
        return RandomValues(rows*cols);
    }

    public static double[] RandomValues(long size) {
        double[] data = new double[(int) (size)];
        for(int i=0; i<data.length; i++) {
            data[i] = rand.nextDouble();
        }
        return data;
    }

    public static double[] SparseRandomValues(long size) {
        return SparseRandomValues(size, 0.9);
    }

    public static double[] SparseRandomValues(long rows, long cols) {
        return SparseRandomValues(rows * cols);
    }

    public static double[] SparseRandomValues(long rows, long cols, double sparsity) {
        return SparseRandomValues(rows * cols, sparsity);
    }

    public static double[] SparseRandomValues(long size, double sparsity) {
        double[] data = new double[(int) (size)];
        for(int i=0; i<data.length; i++) {
            if(rand.nextDouble() > sparsity) {
                data[i] = rand.nextDouble();
            }
            else {
                data[i] = 0.0;
            }
        }
        return data;
    }

    public static double[] AscendingValues(long rows, long cols) {
        return AscendingValues(rows * cols);
    }

    public static double[] AscendingValues(long size) {
        double[] data = new double[(int) (size)];
        for(int i=0; i<data.length; i++) {
            data[i] = i;
        }
        return data;
    }

    public static double[] RandomUniValues(long rows, long cols) {
        return RandomUniValues(rows*cols);
    }

    public static double[] RandomUniValues(long size) {
        double val = rand.nextDouble();
        double[] data = new double[(int) (size)];
        for(int i=0; i<data.length; i++) {
            data[i] = val;
        }
        return data;
    }

}
