package de.tuberlin.pserver.experimental.exp1.types.tests;

import de.tuberlin.pserver.experimental.exp1.memory.Types;
import de.tuberlin.pserver.experimental.exp1.types.matrices.Matrix;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class ParallelMatrixMultiply {

    // ---------------------------------------------------

    public static class Java7MatrixMultiply {
        private static final int SIZE = 4096;
        private static final int THRESHOLD = 8;

        private double[][] a = new double[SIZE][SIZE];
        private double[][] b = new double[SIZE][SIZE];
        private double[][] c = new double[SIZE][SIZE];

        ForkJoinPool forkJoinPool;

        public void initialize() {
            init(a, b, SIZE);
        }

        public void execute() {
            MatrixMultiplyTask mainTask = new MatrixMultiplyTask(a, 0, 0, b, 0, 0, c, 0, 0, SIZE);
            forkJoinPool = new ForkJoinPool();
            forkJoinPool.invoke(mainTask);

            System.out.println("Terminated!");
        }

        public void printResult() {
            check(c, SIZE);

            for (int i = 0; i < SIZE; i++) {
                for (int j = 0; j < SIZE; j++) {
                    System.out.print(c[i][j] + " ");
                }

                System.out.println();
            }
        }

        // To simplify checking, fill with all 1's. Answer should be all n's.
        void init(double[][] a, double[][] b, int n) {
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < n; ++j) {
                    a[i][j] = 1.0F;
                    b[i][j] = 1.0F;
                }
            }
        }

        void check(double[][] c, int n) {
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (c[i][j] != n) {
                        //throw new Error("Check Failed at [" + i + "][" + j + "]: " + c[i][j]);
                        System.out.println("Check Failed at [" + i + "][" + j + "]: " + c[i][j]);
                    }
                }
            }
        }

        private class MatrixMultiplyTask extends RecursiveAction {
            private final double[][] A; // Matrix A
            private final int aRow; // first row of current quadrant of A
            private final int aCol; // first column of current quadrant of A

            private final double[][] B; // Similarly for B
            private final int bRow;
            private final int bCol;

            private final double[][] C; // Similarly for result matrix C
            private final int cRow;
            private final int cCol;

            private final int size;

            MatrixMultiplyTask(double[][] A, int aRow, int aCol, double[][] B,
                               int bRow, int bCol, double[][] C, int cRow, int cCol, int size) {
                this.A = A;
                this.aRow = aRow;
                this.aCol = aCol;
                this.B = B;
                this.bRow = bRow;
                this.bCol = bCol;
                this.C = C;
                this.cRow = cRow;
                this.cCol = cCol;
                this.size = size;
            }

            public ForkJoinTask<?> seq(final ForkJoinTask<?> a, final ForkJoinTask<?> b) {
                return adapt(new Runnable() {
                    public void run() {
                        a.invoke();
                        b.invoke();
                    }
                });
            }

            @Override
            protected void compute() {
                if (size <= THRESHOLD) {
                    multiplyStride2();
                } else {

                    int h = size / 2;

                    invokeAll(
                            seq(new MatrixMultiplyTask(A, aRow, aCol, // A11
                                    B, bRow, bCol, // B11
                                    C, cRow, cCol, // C11
                                    h),

                                new MatrixMultiplyTask(A, aRow, aCol + h, // A12
                                        B, bRow + h, bCol, // B21
                                        C, cRow, cCol, // C11
                                        h)
                            ),

                            seq(new MatrixMultiplyTask(A, aRow, aCol, // A11
                                        B, bRow, bCol + h, // B12
                                        C, cRow, cCol + h, // C12
                                        h),

                                new MatrixMultiplyTask(A, aRow, aCol + h, // A12
                                        B, bRow + h, bCol + h, // B22
                                        C, cRow, cCol + h, // C12
                                        h)
                            ),

                            seq(new MatrixMultiplyTask(A, aRow + h, aCol, // A21
                                        B, bRow, bCol, // B11
                                        C, cRow + h, cCol, // C21
                                        h),

                                new MatrixMultiplyTask(A, aRow + h, aCol + h, // A22
                                        B, bRow + h, bCol, // B21
                                        C, cRow + h, cCol, // C21
                                        h)
                            ),

                            seq(new MatrixMultiplyTask(A, aRow + h, aCol, // A21
                                        B, bRow, bCol + h, // B12
                                        C, cRow + h, cCol + h, // C22
                                        h),

                                new MatrixMultiplyTask(A, aRow + h, aCol + h, // A22
                                        B, bRow + h, bCol + h, // B22
                                        C, cRow + h, cCol + h, // C22
                                        h)
                            )
                    );
                }
            }

            /**
             * Version of matrix multiplication that steps 2 rows and columns at a
             * time. Adapted from Cilk demos. Note that the results are added into
             * C, not just set into C. This works well here because Java array
             * elements are created with all zero values.
             **/

            void multiplyStride2() {
                for (int j = 0; j < size; j += 2) {
                    for (int i = 0; i < size; i += 2) {

                        double[] a0 = A[aRow + i];
                        double[] a1 = A[aRow + i + 1];

                        double s00 = 0.0F;
                        double s01 = 0.0F;
                        double s10 = 0.0F;
                        double s11 = 0.0F;

                        for (int k = 0; k < size; k += 2) {

                            double[] b0 = B[bRow + k];

                            s00 += a0[aCol + k] * b0[bCol + j];
                            s10 += a1[aCol + k] * b0[bCol + j];
                            s01 += a0[aCol + k] * b0[bCol + j + 1];
                            s11 += a1[aCol + k] * b0[bCol + j + 1];

                            double[] b1 = B[bRow + k + 1];

                            s00 += a0[aCol + k + 1] * b1[bCol + j];
                            s10 += a1[aCol + k + 1] * b1[bCol + j];
                            s01 += a0[aCol + k + 1] * b1[bCol + j + 1];
                            s11 += a1[aCol + k + 1] * b1[bCol + j + 1];
                        }

                        C[cRow + i][cCol + j] += s00;
                        C[cRow + i][cCol + j + 1] += s01;
                        C[cRow + i + 1][cCol + j] += s10;
                        C[cRow + i + 1][cCol + j + 1] += s11;
                    }
                }
            }
        }
    }

    // ---------------------------------------------------

    public static class OwnJava7MatrixMultiply {
        private static final int SIZE = 4096;
        private static final int THRESHOLD = 8;

        private final DenseDoubleMatrixOld a = new DenseDoubleMatrixOld(SIZE, SIZE, Matrix.BlockLayout.ROW_LAYOUT);
        private final DenseDoubleMatrixOld b = new DenseDoubleMatrixOld(SIZE, SIZE, Matrix.BlockLayout.ROW_LAYOUT);
        private final DenseDoubleMatrixOld c = new DenseDoubleMatrixOld(SIZE, SIZE, Matrix.BlockLayout.ROW_LAYOUT);

        ForkJoinPool forkJoinPool;

        public void initialize() {
            init(a, b, SIZE);
        }

        public void execute() {
            MatrixMultiplyTask mainTask = new MatrixMultiplyTask(a, 0, 0, b, 0, 0, c, 0, 0, SIZE);
            forkJoinPool = new ForkJoinPool();
            forkJoinPool.invoke(mainTask);

            System.out.println("Terminated!");
        }

        public void printResult() {
            check(c, SIZE);

            for (int i = 0; i < SIZE && i <= 10; i++) {
                for (int j = 0; j < SIZE && j <= 10; j++) {
                    if (j == 10) {
                        System.out.print("...");
                    } else {
                        System.out.print(c.get(i, j) + " ");
                    }
                }

                if (i == 10) {
                    System.out.println();
                    for (int k = 0; k < 10; k++) System.out.print(" ... ");
                }

                System.out.println();
            }

            System.out.println();
        }

        // To simplify checking, fill with all 1's. Answer should be all n's.
        static void init(final DenseDoubleMatrixOld a, final DenseDoubleMatrixOld b, int n) {
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < n; ++j) {
                    a.set(i, j, 1.0);
                    b.set(i, j, 1.0);
                }
            }
        }

        static void check(final DenseDoubleMatrixOld c, int n) {
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (c.get(i, j) != n) {
                        throw new Error("Check Failed at [" + i + "][" + j + "]: " + c.get(i, j));
                        //System.out.println("Check Failed at [" + i + "][" + j + "]: " + c[i][j]);
                    }
                }
            }
        }

        private class MatrixMultiplyTask extends RecursiveAction {

            private final DenseDoubleMatrixOld A; // Matrix A
            private final int aRow; // first row of current quadrant of A
            private final int aCol; // first column of current quadrant of A

            private final DenseDoubleMatrixOld B; // Similarly for B
            private final int bRow;
            private final int bCol;

            private final DenseDoubleMatrixOld C; // Similarly for result matrix C
            private final int cRow;
            private final int cCol;

            private final int size;

            public MatrixMultiplyTask(final DenseDoubleMatrixOld A, int aRow, int aCol, final DenseDoubleMatrixOld B,
                                      int bRow, int bCol, final DenseDoubleMatrixOld C, int cRow, int cCol, int size) {

                this.A = A;
                this.aRow = aRow;
                this.aCol = aCol;
                this.B = B;
                this.bRow = bRow;
                this.bCol = bCol;
                this.C = C;
                this.cRow = cRow;
                this.cCol = cCol;
                this.size = size;
            }

            public ForkJoinTask<?> seq(final ForkJoinTask<?> a, final ForkJoinTask<?> b) {
                return adapt(new Runnable() {
                    public void run() {
                        a.invoke();
                        b.invoke();
                    }
                });
            }

            @Override
            protected void compute() {
                if (size <= THRESHOLD) {
                    multiplyStride2();
                } else {

                    int h = size / 2;

                    invokeAll(
                            seq(new MatrixMultiplyTask(A, aRow, aCol, // A11
                                            B, bRow, bCol, // B11
                                            C, cRow, cCol, // C11
                                            h),

                                    new MatrixMultiplyTask(A, aRow, aCol + h, // A12
                                            B, bRow + h, bCol, // B21
                                            C, cRow, cCol, // C11
                                            h)
                            ),

                            seq(new MatrixMultiplyTask(A, aRow, aCol, // A11
                                            B, bRow, bCol + h, // B12
                                            C, cRow, cCol + h, // C12
                                            h),

                                    new MatrixMultiplyTask(A, aRow, aCol + h, // A12
                                            B, bRow + h, bCol + h, // B22
                                            C, cRow, cCol + h, // C12
                                            h)
                            ),

                            seq(new MatrixMultiplyTask(A, aRow + h, aCol, // A21
                                            B, bRow, bCol, // B11
                                            C, cRow + h, cCol, // C21
                                            h),

                                    new MatrixMultiplyTask(A, aRow + h, aCol + h, // A22
                                            B, bRow + h, bCol, // B21
                                            C, cRow + h, cCol, // C21
                                            h)
                            ),

                            seq(new MatrixMultiplyTask(A, aRow + h, aCol, // A21
                                            B, bRow, bCol + h, // B12
                                            C, cRow + h, cCol + h, // C22
                                            h),

                                    new MatrixMultiplyTask(A, aRow + h, aCol + h, // A22
                                            B, bRow + h, bCol + h, // B22
                                            C, cRow + h, cCol + h, // C22
                                            h)
                            )
                    );
                }
            }

            /**
             * Version of matrix multiplication that steps 2 rows and columns at a
             * time. Adapted from Cilk demos. Note that the results are added into
             * C, not just set into C. This works well here because Java array
             * elements are created with all zero values.
             **/
            void multiplyStride2() {

                final byte[] dataA = A.getBuffer().getRawData();
                final byte[] dataB = B.getBuffer().getRawData();
                final byte[] dataC = C.getBuffer().getRawData();
                final long ds = Types.DOUBLE_TYPE_INFO.size();

                for (int j = 0; j < size; j += 2) {
                    for (int i = 0; i < size; i += 2) {

                        double s00 = 0.0;
                        double s01 = 0.0;
                        double s10 = 0.0;
                        double s11 = 0.0;

                        for (int k = 0; k < size; k += 2) {
                            
                            {
                                //final double a  = UnsafeOp.unsafe.getDouble(A.getBuffer().getRawData(), (long)(((aRow + i) * (aCol + k) + (aCol + k)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));
                                //final double a1 = UnsafeOp.unsafe.getDouble(A.getBuffer().getRawData(), (long) (((aRow + i + 1) * (aCol + k) + (aCol + k)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));
                                //final double b  = UnsafeOp.unsafe.getDouble(B.getBuffer().getRawData(), (long) (((bRow + k) * (bCol + j) + (bCol + j)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));
                                //final double b1 = UnsafeOp.unsafe.getDouble(B.getBuffer().getRawData(), (long) (((bRow + k) * (bCol + j + 1) + (bCol + j + 1)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));

                                //final long offAxTmp = (aCol + k) + (aCol + k);
                                //final long offA     = ((aRow + i)     * offAxTmp) * ds + UnsafeOp.BYTE_ARRAY_OFFSET;
                                //final long offA1    = ((aRow + i + 1) * offAxTmp) * ds + UnsafeOp.BYTE_ARRAY_OFFSET;

                                //final long offBxTmp = (bRow + k);
                                //final long offB     = (offBxTmp * (bCol + j) + (bCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET;
                                //final long offB1    = (offBxTmp * (bCol + j + 1) + (bCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET;

                                //s00 += UnsafeOp.unsafe.getDouble(dataA, offA)  * UnsafeOp.unsafe.getDouble(dataB, offB);
                                //s10 += UnsafeOp.unsafe.getDouble(dataA, offA1) * UnsafeOp.unsafe.getDouble(dataB, offB);
                                //s01 += UnsafeOp.unsafe.getDouble(dataA, offA)  * UnsafeOp.unsafe.getDouble(dataB, offB1);
                                //s11 += UnsafeOp.unsafe.getDouble(dataA, offA1) * UnsafeOp.unsafe.getDouble(dataB, offB1);

                                final double a  = A.get(aRow + i, aCol + k);
                                final double a1 = A.get(aRow + i + 1, aCol + k);
                                final double b  = B.get(bRow + k, bCol + j);
                                final double b1 = B.get(bRow + k, bCol + j + 1);

                                s00 += a * b;
                                s10 += a1 * b;
                                s01 += a * b1;
                                s11 += a1 * b1;
                            }

                            {
                                //final double a  = UnsafeOp.unsafe.getDouble(A.getBuffer().getRawData(), (long) (((aRow + i) * (aCol + k + 1) + (aCol + k + 1)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));
                                //final double a1 = UnsafeOp.unsafe.getDouble(A.getBuffer().getRawData(), (long) (((aRow + i + 1) * (aCol + k + 1) + (aCol + k + 1)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));
                                //final double b  = UnsafeOp.unsafe.getDouble(B.getBuffer().getRawData(), (long) (((bRow + k + 1) * (bCol + j) + (bCol + j)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));
                                //final double b1 = UnsafeOp.unsafe.getDouble(B.getBuffer().getRawData(), (long) (((bRow + k + 1) * (bCol + j + 1) + (bCol + j + 1)) * size + UnsafeOp.BYTE_ARRAY_OFFSET));

                                //final long offAxTmp = (aCol + k + 1) + (aCol + k + 1);
                                //final long offA     = (((aRow + i) * offAxTmp)     * ds + UnsafeOp.BYTE_ARRAY_OFFSET);
                                //final long offA1    = (((aRow + i + 1) * offAxTmp) * ds + UnsafeOp.BYTE_ARRAY_OFFSET);

                                //final long offBxTmp = (bRow + k + 1);
                                //final long offB     = ((offBxTmp * (bCol + j) + (bCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET);
                                //final long offB1    = ((offBxTmp * (bCol + j + 1) + (bCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET);

                                //s00 += UnsafeOp.unsafe.getDouble(dataA, offA)  * UnsafeOp.unsafe.getDouble(dataB, offB);
                                //s10 += UnsafeOp.unsafe.getDouble(dataA, offA1) * UnsafeOp.unsafe.getDouble(dataB, offB);
                                //s01 += UnsafeOp.unsafe.getDouble(dataA, offA)  * UnsafeOp.unsafe.getDouble(dataB, offB1);
                                //s11 += UnsafeOp.unsafe.getDouble(dataA, offA1) * UnsafeOp.unsafe.getDouble(dataB, offB1);

                                final double a  = A.get(aRow + i, aCol + k + 1);
                                final double a1 = A.get(aRow + i + 1, aCol + k + 1);
                                final double b  = B.get(bRow + k + 1, bCol + j);
                                final double b1 = B.get(bRow + k + 1, bCol + j + 1);

                                s00 += a *  b;
                                s10 += a1 * b;
                                s01 += a *  b1;
                                s11 += a1 * b1;
                            }
                        }
                        
                        final double _s00 = C.get(cRow + i, cCol + j) + s00;
                        final double _s01 = C.get(cRow + i, cCol + j + 1) + s01;
                        final double _s10 = C.get(cRow + i + 1, cCol + j) + s10;
                        final double _s11 = C.get(cRow + i + 1, cCol + j + 1) + s11;

                        C.set(cRow + i,     cCol + j,       _s00);
                        C.set(cRow + i,     cCol + j + 1,   _s01);
                        C.set(cRow + i + 1, cCol + j,       _s10);
                        C.set(cRow + i + 1, cCol + j + 1,   _s11);

                        //final double _s00 = UnsafeOp.unsafe.getDouble(dataC, (((cRow + i) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s00;
                        //final double _s01 = UnsafeOp.unsafe.getDouble(dataC, (((cRow + i) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s01;
                        //final double _s10 = UnsafeOp.unsafe.getDouble(dataC, (((cRow + i + 1) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s10;
                        //final double _s11 = UnsafeOp.unsafe.getDouble(dataC, (((cRow + i + 1) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s11;

                        //UnsafeOp.unsafe.putDouble(dataC, (long)(((cRow + i) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),                s00);
                        //UnsafeOp.unsafe.putDouble(dataC, (long)(((cRow + i) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),        s01);
                        //UnsafeOp.unsafe.putDouble(dataC, (long)(((cRow + i + 1) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),            s10);
                        //UnsafeOp.unsafe.putDouble(dataC, (long)(((cRow + i + 1) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),    s11);

                        //UnsafeOp.unsafe.putDouble(dataC, (((cRow + i) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),                UnsafeOp.unsafe.getDouble(dataC, (((cRow + i) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s00);
                        //UnsafeOp.unsafe.putDouble(dataC, (((cRow + i) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),        UnsafeOp.unsafe.getDouble(dataC, (((cRow + i) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s01);
                        //UnsafeOp.unsafe.putDouble(dataC, (((cRow + i + 1) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),            UnsafeOp.unsafe.getDouble(dataC, (((cRow + i + 1) * (cCol + j) + (cCol + j)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s10);
                        //UnsafeOp.unsafe.putDouble(dataC, (((cRow + i + 1) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET),    UnsafeOp.unsafe.getDouble(dataC, (((cRow + i + 1) * (cCol + j + 1) + (cCol + j + 1)) * ds + UnsafeOp.BYTE_ARRAY_OFFSET)) + s11);

                        //C.set(cRow + i,     cCol + j,       C.get(cRow + i, cCol + j)           + s00);
                        //C.set(cRow + i,     cCol + j + 1,   C.get(cRow + i, cCol + j + 1)       + s01);
                        //C.set(cRow + i + 1, cCol + j,       C.get(cRow + i + 1, cCol + j)       + s10);
                        //C.set(cRow + i + 1, cCol + j + 1,   C.get(cRow + i + 1, cCol + j + 1)   + s11);
                    }
                }
            }
        }
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {

        // 3,1s own             - 2048 x 2048
        // 2,6s double arrays   - 2048 x 2048

        // 24,8s own            - 4096 x 4096
        // 19,9s double arrays  - 4096 x 4096

        final OwnJava7MatrixMultiply pmm = new OwnJava7MatrixMultiply();

        pmm.initialize();

        final long start = System.currentTimeMillis();

        pmm.execute();

        final long end = System.currentTimeMillis();
        final long time = end - start;
        System.out.println("elapsed time: " + time + "ms");
    }
}