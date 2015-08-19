package de.tuberlin.pserver.runtime.delta;


import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.SlotContext;

import java.util.ArrayList;
import java.util.List;

public class MatrixDelta implements SharedObject {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public byte[] buffer;

    public List<byte[]> compressedDeltas;

    private Object owner;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixDelta(final SlotContext slotContext, final long rows, final long cols) {
        this.buffer = new byte[(int)(rows * cols * Double.BYTES)];
        this.compressedDeltas = new ArrayList<>(slotContext.programContext.perNodeDOP);
        for (int i = 0; i < slotContext.programContext.perNodeDOP; ++i)
            compressedDeltas.add(null);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public void setOwner(final Object owner) { this.owner = owner; }

    @Override public Object getOwner() { return owner; }

    @Override public long sizeOf() { return buffer.length; }

    @Override public double[] toArray() { return null; }

    @Override public void setArray(final double[] data) { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------
}
