package de.tuberlin.pserver.types.collection.implementation;


import de.tuberlin.pserver.types.collection.metadata.AbstractDistributedCollectionType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;
import de.tuberlin.pserver.types.metadata.InternalData;

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

    public DistributedCollection(int nodeID, int[] nodes, DistributionScheme distributionScheme) {
        super(nodeID, nodes, distributionScheme);
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
