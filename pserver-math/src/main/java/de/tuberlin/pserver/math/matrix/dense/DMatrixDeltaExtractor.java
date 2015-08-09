package de.tuberlin.pserver.math.matrix.dense;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;
import org.apache.commons.lang3.tuple.Triple;
import sun.misc.Unsafe;

public final class DMatrixDeltaExtractor {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static interface DeltaFilter {

        public abstract boolean filter(final long i, final long j, final double ov, final double nv);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public static final double SEQ_MARKER = Double.POSITIVE_INFINITY;

    private final DMatrix currentState;

    private final DMatrix previousState;

    private final DeltaFilter filter;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DMatrixDeltaExtractor(final DMatrix currentState,
                                 final DMatrix previousState,
                                 final DeltaFilter filter) {

        this.currentState  = Preconditions.checkNotNull(currentState);
        this.previousState = Preconditions.checkNotNull(previousState);
        this.filter        = Preconditions.checkNotNull(filter);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Triple<Integer, Integer, double[]> extractDeltas() { return extractDeltas(1, 0); }
    public Triple<Integer, Integer, double[]> extractDeltas(final int numOfInstances, final int instanceID) {
        final int parLength = currentState.toArray().length / numOfInstances;
        final int s = parLength * instanceID;
        final int e = (instanceID == numOfInstances - 1) ? currentState.toArray().length - 1 : s + parLength;
        long rleOffset = Unsafe.ARRAY_DOUBLE_BASE_OFFSET + s;
        for (int i = s; i < e; ++i) {
            int runLength = 1;
            while (i + 1 < e && filterAndGetValue(i) == filterAndGetValue(i + 1)) {
                runLength++; i++;
            }
            final double value = currentState.toArray()[i];
            if (runLength >= 3)
                rleOffset = write(previousState.toArray(), rleOffset, runLength, value);
            else
                if (value != SEQ_MARKER)
                    rleOffset = write(previousState.toArray(), rleOffset, value);
                else
                    rleOffset = write(previousState.toArray(), rleOffset, 1, SEQ_MARKER);
        }
        return Triple.of((Unsafe.ARRAY_DOUBLE_BASE_OFFSET + s), (int)rleOffset, previousState.toArray());
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private double filterAndGetValue(final int i) {
        final long row = i / currentState.numCols();
        final long col = i % currentState.numCols();
        return filter.filter(row, col, previousState.toArray()[i], currentState.toArray()[i])
                ? currentState.toArray()[i] : 0.0;
    }

    private long write(final double[] target, long rleOffset, final int runLength, final double value) {
        UnsafeOp.unsafe.putDouble(target, rleOffset, SEQ_MARKER);
        UnsafeOp.unsafe.putInt(target, rleOffset + Double.BYTES, runLength);
        UnsafeOp.unsafe.putDouble(target, rleOffset + Double.BYTES + Integer.BYTES, value);
        rleOffset += Double.BYTES + Integer.BYTES + Double.BYTES;
        return rleOffset;
    }

    private long write(final double[] target, long rleOffset, final double value) {
        UnsafeOp.unsafe.putDouble(target, rleOffset, value);
        rleOffset += Double.BYTES;
        return rleOffset;
    }
}
