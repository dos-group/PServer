package de.tuberlin.pserver.runtime.delta;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.runtime.SlotContext;
import sun.misc.Unsafe;

public final class LZ4MatrixDeltaExtractor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DMatrix currentState;

    private final Compressor compressor;

    private final MatrixDelta delta;

    private MatrixDeltaFilter filter;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LZ4MatrixDeltaExtractor(final DMatrix currentState, final MatrixDelta delta) throws Exception {
        this.currentState  = Preconditions.checkNotNull(currentState);
        this.delta         = Preconditions.checkNotNull(delta);
        this.compressor    = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void extractDeltas(final SlotContext slotContext) throws Exception {
        Preconditions.checkNotNull(filter);

        slotContext.CF.iterate().parExe(currentState, (e, i, j, v) -> {
            final double newVal = currentState.get(i, j);
            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * currentState.numCols() + j) * Double.BYTES);
            final double oldVal = UnsafeOp.unsafe.getDouble(delta.buffer, bufferOffset);
            final double deltaVal = filter.filter(i, j, oldVal, newVal) ? newVal : 0.0;
            UnsafeOp.unsafe.putDouble(delta.buffer, bufferOffset, deltaVal);
        });

        final int offset = (delta.buffer.length / slotContext.programContext.perNodeDOP) * slotContext.slotID;
        int length = delta.buffer.length / slotContext.programContext.perNodeDOP;
        length = (slotContext.slotID == slotContext.programContext.perNodeDOP - 1)
                ? (length + (delta.buffer.length % 2)) : length;

        System.out.println("LZ4 compress( offset = " + offset + ", length = " + length + " )");

        delta.compressedDeltas.set(slotContext.slotID, compressor.compress(delta.buffer, offset, length)); // TODO: Creates a new buffer...

        slotContext.CF.iterate().parExe(currentState, (e, i, j, v) -> {
            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + (i * currentState.numCols() + j) * Double.BYTES;
            UnsafeOp.unsafe.putDouble(delta.buffer, bufferOffset, v);
        });
    }

    public void setDeltaFilter(final MatrixDeltaFilter filter) { this.filter = Preconditions.checkNotNull(filter); }
}