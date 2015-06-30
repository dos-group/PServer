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

    @Override public void allocateMemory(int instanceID) {}

    @Override public Segment[] getSegments(int[] segmentIndices, int instanceID) { throw new UnsupportedOperationException(); }

    @Override public void putSegments(Segment[] segments, int instanceID) { throw new UnsupportedOperationException(); }
}