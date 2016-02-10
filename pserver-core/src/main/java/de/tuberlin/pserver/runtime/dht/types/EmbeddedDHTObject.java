package de.tuberlin.pserver.runtime.dht.types;

import de.tuberlin.pserver.types.DistributedType;

public class EmbeddedDHTObject<T extends DistributedType> extends AbstractBufferedDHTObject {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //protected double[] data;

    public final T object;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public EmbeddedDHTObject(final T object) {
        super((int)object.sizeOf(), false);
        this.object = object;
        //this.data   = object.toArray();
        this.object.owner(this);
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