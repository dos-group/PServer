package de.tuberlin.pserver.types.collection;


import de.tuberlin.pserver.types.InternalData;

import java.util.ArrayList;
import java.util.List;

// TODO: READ IT - http://atomix.io/atomix/user-manual/resources/

public final class DistributedCollection<T> extends AbstractDistributedCollectionType {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<T> data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedCollection(int nodeID, int[] nodes) {
        super(nodeID, nodes);
        this.data = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Distributed Type Metadata.
    // ---------------------------------------------------

    @Override
    public long sizeOf() { throw new UnsupportedOperationException(); }

    @SuppressWarnings("unchecked")
    @Override public InternalData<List<T>> internal() { return new InternalData<>(data); }
}
