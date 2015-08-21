package de.tuberlin.pserver.runtime.delta;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;

import java.util.ArrayList;
import java.util.List;

public class MatrixDelta implements SharedObject {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private transient Object owner;

    public transient Matrix matrix;

    public transient final byte[] buffer;

    public final int nodeID;

    public final List<byte[]> compressedDeltas;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixDelta(final SlotContext slotContext, final Matrix matrix) {
        this.matrix = Preconditions.checkNotNull(matrix);
        this.buffer = new byte[(int)(matrix.numRows() * matrix.numCols() * Double.BYTES)];
        this.nodeID = slotContext.programContext.runtimeContext.nodeID;
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
