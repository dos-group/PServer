package de.tuberlin.pserver.app.types;

import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.math.MObject;

public class MObjectValue<T extends MObject> extends AbstractBufferValue {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected double[] data;

    public final T object;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MObjectValue(final T object) {
        super((int)object.sizeOf(), false);
        this.object = object;
        this.data = object.toArray();
        this.object.setOwner(this);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override public void compress() { throw new UnsupportedOperationException(); }

    @Override public void decompress() { throw new UnsupportedOperationException(); }

    @Override public void allocateMemory(int nodeID) {}

    @Override public Segment[] getSegments(int[] segmentIndices, int nodeID) { throw new UnsupportedOperationException(); }

    @Override public void putSegments(Segment[] segments, int nodeID) { throw new UnsupportedOperationException(); }
}