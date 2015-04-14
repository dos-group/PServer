package de.tuberlin.pserver.math.experimental.types.tests;

public class SimpleMatrixMultiply {

    private SimpleMatrixMultiply() {}

    public static void main(final String[] args) {

        /*
        final int SIZE = 4096;

        final DenseDoubleMatrixOld a = new DenseDoubleMatrixOld(SIZE, SIZE, Matrix.BlockLayout.ROW_LAYOUT);
        final DenseDoubleMatrixOld b = new DenseDoubleMatrixOld(SIZE, SIZE, Matrix.BlockLayout.ROW_LAYOUT);
        final DenseDoubleMatrixOld c = new DenseDoubleMatrixOld(SIZE, SIZE, Matrix.BlockLayout.ROW_LAYOUT);

        final int m = (int)a.numRows();
        final int n = (int)a.numCols();
        final int p = (int)b.numRows();
        final int q = (int)b.numCols();

        if (n != p) throw new IllegalStateException("Illegal matrix dimensions.");

        final long start = System.currentTimeMillis();

        double tmp = 0.0;

        for (int i = 0; i < m; ++i) {

            for (int j = 0; j < q; ++j) {

                for (int k = 0; k < p; ++k) {

                    tmp += a.get(i, k) * b.get(k, j);
                }

                c.set(i, j, tmp);
                tmp = 0.0;
            }
        }

        final long epilogue = System.currentTimeMillis();
        final long time = epilogue - start;
        System.out.println("elapsed time: " + time + "ms");
        // 592594ms
        */


        // ---------------------------------------------------
        /*
        final double[][] a = new double[SIZE][SIZE];
        final double[][] b = new double[SIZE][SIZE];
        final double[][] c = new double[SIZE][SIZE];

        final int m = a.size;
        final int n = a[0].size;
        final int p = b.size;
        final int q = b[0].size;

        if (n != p) throw new IllegalStateException("Illegal matrix dimensions.");

        final long start = System.currentTimeMillis();

        double tmp = 0.0;

        for (int i = 0; i < m; ++i) {

            for (int j = 0; j < q; ++j) {

                for (int k = 0; k < p; ++k) {

                    tmp += a[i][k] * b[k][j];
                }

                c[i][j] = tmp;
                tmp = 0.0;
            }
        }

        final long epilogue = System.currentTimeMillis();
        final long time = epilogue - start;
        System.out.println("elapsed time: " + time + "ms");*/
    }
}
