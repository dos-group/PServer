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

    public static double[] AscendingValues(long rows, long cols) {
        return AscendingValues(rows*cols);
    }

    public static double[] AscendingValues(long size) {
        double[] data = new double[(int) (size)];
        for(int i=0; i<data.length; i++) {
            data[i] = i;
        }
        return data;
    }

}
