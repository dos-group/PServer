package de.tuberlin.pserver.runtime.state.update;


import de.tuberlin.pserver.math.SharedObject;

import java.util.ArrayList;
import java.util.List;

public class MatrixDeltaUpdate implements SharedObject {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<byte[]> deltas;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MatrixDeltaUpdate(final int dop) {
        this.deltas = new ArrayList<>(dop);
        for (int i = 0; i < dop; ++i)
            deltas.add(null);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setDelta(final int index, final byte[] deltaData) { deltas.set(index, deltaData); }

    public byte[] getDelta(final int index) { return deltas.get(index); }

    // ---------------------------------------------------

    @Override public void setOwner(final Object owner) {}

    @Override public Object getOwner() { return null; }

    @Override public long sizeOf() { return 0; }

    @Override public double[] toArray() { return null; }

    @Override public void setArray(final double[] data) { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------
}
