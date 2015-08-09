package de.tuberlin.pserver.math.matrix.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.Buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DeltaDMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static interface DeltaPolicy {

        public abstract boolean eval(double o, double n);
    }

    public static final class MatrixDelta {

        private static final int ENTRY_SIZE = Integer.BYTES + Integer.BYTES + Double.BYTES;

        private static final int DELTA_BUFFER_SIZE = ENTRY_SIZE * 4096;

        private List<byte[]> deltaBufferList = new ArrayList<>();

        private Buffer currentBuffer;

        private int offset;

        public void addDelta(int row, int col, double delta) {
            if (offset >= DELTA_BUFFER_SIZE || offset == 0)
                allocBuffer();
            currentBuffer.putInt(offset, row);
            currentBuffer.putInt(offset + Integer.BYTES, col);
            currentBuffer.putDouble(offset + Integer.BYTES + Integer.BYTES, delta);
            offset += ENTRY_SIZE;
        }

        public int getSize() { return deltaBufferList.size() > 1 ? deltaBufferList.size() * DELTA_BUFFER_SIZE + offset : offset; }

        public List<byte[]> getDeltas() { return deltaBufferList; }

        public void reset() {
            deltaBufferList.clear();
            allocBuffer();
        }

        private void allocBuffer() {
            final byte[] buffer = new byte[DELTA_BUFFER_SIZE];
            deltaBufferList.add(buffer);
            currentBuffer = new Buffer(buffer);
            offset = 0;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final boolean storeDeltaHistory;

    private final DeltaPolicy deltaPolicy;

    private final List<MatrixDelta[]> versionedDeltas;

    private final int[] writerEpochs;

    private final MatrixDelta[] currentDelta;

    private final double[] data;

    public final int rows;

    public final int cols;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DeltaDMatrix(final int rows, final int cols,
                        final int numOfWriters,
                        final boolean storeDeltaHistory,
                        final DeltaPolicy deltaPolicy) {

        Preconditions.checkArgument(rows * cols < Integer.MAX_VALUE);

        this.data = new double[rows * cols];
        this.rows = rows;
        this.cols = cols;

        this.storeDeltaHistory = storeDeltaHistory;
        this.deltaPolicy = Preconditions.checkNotNull(deltaPolicy);
        this.versionedDeltas = new ArrayList<>();
        this.writerEpochs = new int[numOfWriters];
        this.currentDelta = new MatrixDelta[numOfWriters];

        for (int i = 0; i < numOfWriters; ++i)
            currentDelta[i] = new MatrixDelta();

        versionedDeltas.add(currentDelta);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void enterNextEpoch(int epoch, int writerID) {
        final int numOfWriters = writerEpochs.length;
        writerEpochs[writerID] = epoch;
        synchronized (this) {
            if (storeDeltaHistory) {
                if (writerEpochs[writerID] >= versionedDeltas.size()) {
                    final MatrixDelta[] deltasForEpoch = new MatrixDelta[numOfWriters];
                    for (int i = 0; i < numOfWriters; ++i)
                        deltasForEpoch[i] = new MatrixDelta();
                    versionedDeltas.add(deltasForEpoch);
                }
                currentDelta[writerID] = versionedDeltas.get(writerEpochs[writerID])[writerID];
            } else {
                currentDelta[writerID].reset();
            }
        }
    }

    public void set(int writerID, int row, int col, double val) {
        if (deltaPolicy.eval(data[row * cols + col], val)) {
            currentDelta[writerID].addDelta(row, col, val - data[row * cols + col]);
        }
        data[row * cols + col] = val;
    }

    public double get(int row, int col) { return data[row * cols + col]; }

    public MatrixDelta getDelta(int epoch, int writerID) { return versionedDeltas.get(epoch)[writerID]; }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) throws Exception {

        final int MTX_ROWS = 2000;

        final int MTX_COLS = 2000;

        final int NUM_OF_WRITERS = 8;

        final int NUM_OF_EPOCHS = 8;

        final DeltaPolicy dp = (o, n) -> Math.abs(o - n) > 0.3;

        final DeltaDMatrix m = new DeltaDMatrix(MTX_ROWS, MTX_COLS, NUM_OF_WRITERS, true, dp);

        final ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_WRITERS);

        final List<Callable<Void>> writerThreads = new ArrayList<>();

        for (int i = 0; i < NUM_OF_WRITERS; ++i) {

            final int writerID = i;

            writerThreads.add(() -> {
                        final Random rand = new Random();
                        for (int epoch = 0; epoch < NUM_OF_EPOCHS; ++epoch) {

                            m.enterNextEpoch(epoch, writerID);

                            for (int j = 0; j < MTX_ROWS; ++j) {
                                for (int k = 0; k < MTX_COLS; ++k) {
                                    m.set(writerID, j, k, rand.nextInt(100 - (epoch * (100 / NUM_OF_EPOCHS))) / 100.0);
                                }
                            }

                        }
                        return null;
                    }
            );
        }

        final long start = System.currentTimeMillis();

        executor.invokeAll(writerThreads);

        final long end = System.currentTimeMillis();

        System.out.println("elapsed time = " + (end - start) + "ms");

        for (int e = 0; e < NUM_OF_EPOCHS; ++e) {
            for (int w = 0; w < NUM_OF_WRITERS; ++w)
                System.out.println("EPOCH("+ e + ") => WRITER(" + w + ") => \t"
                        + m.getDelta(e, w).getDeltas().size()
                        /*+ " => SIZE => \t" + m.getDelta(e, w).getSize()*/
                        + "\t DIFF => \t" + (int)((m.getDelta(e, w).getSize() / (double)(MTX_ROWS * MTX_COLS * Double.BYTES)) * 100) + " %");

            System.out.println();
        }

        executor.shutdown();
    }
}
