package de.tuberlin.pserver.runtime.delta;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;

public final class MatrixDeltaManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Compressor compressor;

    private final EmbeddedDHTObject<MatrixDelta> dhtObject;

    private final MatrixDelta delta;

    private MatrixDeltaFilter filter;

    private MatrixDeltaMerger merger;

    private List<MatrixDelta> remoteDeltas;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixDeltaManager(final EmbeddedDHTObject<MatrixDelta> dhtObject) throws Exception {
        this.dhtObject     = Preconditions.checkNotNull(dhtObject);
        this.delta         = dhtObject.object;
        this.compressor    = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);
        this.remoteDeltas  = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setDeltaFilter(final MatrixDeltaFilter filter) { this.filter = Preconditions.checkNotNull(filter); }

    public void setDeltaMerger(final MatrixDeltaMerger merger) { this.merger = Preconditions.checkNotNull(merger); }

    public void setRemoteDeltas(final List<MatrixDelta> remoteDeltas) { this.remoteDeltas =  Preconditions.checkNotNull(remoteDeltas); }

    // ---------------------------------------------------

    public void extractDeltas(final SlotContext slotContext) throws Exception {
        Preconditions.checkNotNull(filter);

        slotContext.CF.syncSlots();

        slotContext.CF.iterate().parExe(delta.matrix, (e, i, j, v) -> {
            final double newVal = delta.matrix.get(i, j);
            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * delta.matrix.numCols() + j) * Double.BYTES);
            final double oldVal = UnsafeOp.unsafe.getDouble(delta.buffer, bufferOffset);
            final double deltaVal = filter.filter(i, j, oldVal, newVal) ? newVal : Double.NaN;
            UnsafeOp.unsafe.putDouble(delta.buffer, bufferOffset, deltaVal);
        });

        final int offset = (delta.buffer.length / slotContext.programContext.perNodeDOP) * slotContext.slotID;
        int length = delta.buffer.length / slotContext.programContext.perNodeDOP;
        length = (slotContext.slotID == slotContext.programContext.perNodeDOP - 1)
                ? (length + (delta.buffer.length % 2)) : length;

        synchronized (dhtObject.lock) {
            delta.compressedDeltas.set(slotContext.slotID, compressor.compress(delta.buffer, offset, length)); // TODO: Creates a new buffer...
        }

        slotContext.CF.iterate().parExe(delta.matrix, (e, i, j, v) -> {
            final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + (i * delta.matrix.numCols() + j) * Double.BYTES;
            UnsafeOp.unsafe.putDouble(delta.buffer, bufferOffset, v);
        });
    }

    public void integrateDelta(final SlotContext slotContext) throws Exception {
        for (final MatrixDelta remoteDelta : remoteDeltas) {
            final byte[] compressedDelta = remoteDelta.compressedDeltas.get(slotContext.slotID);
            if (compressedDelta != null) {

                final int offset = (delta.buffer.length / slotContext.programContext.perNodeDOP) * slotContext.slotID;
                int length = delta.buffer.length / slotContext.programContext.perNodeDOP;
                length = (slotContext.slotID == slotContext.programContext.perNodeDOP - 1)
                        ? (length + (delta.buffer.length % 2)) : length;

                compressor.decompress(compressedDelta, 0, delta.buffer, offset, length);

                slotContext.CF.iterate().parExe(delta.matrix, (e, i, j, v) -> {
                    final long bufferOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + ((i * delta.matrix.numCols() + j) * Double.BYTES);
                    final double remoteVal = UnsafeOp.unsafe.getDouble(delta.buffer, bufferOffset);
                    if (remoteVal != Double.NaN)
                        merger.mergeElement(i, j, v, remoteVal);
                });
            }
        }
    }
}