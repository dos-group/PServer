package de.tuberlin.pserver.types.collection.implementation;


import de.tuberlin.pserver.types.collection.typeinfo.AbstractCollectionTypeInfo;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;
import de.tuberlin.pserver.types.typeinfo.properties.InternalData;

import java.util.ArrayList;
import java.util.List;

// TODO: READ IT - http://atomix.io/atomix/user-manual/resources/

public final class DCollection<T> extends AbstractCollectionTypeInfo {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<T> data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DCollection(int nodeID, int[] nodes, Class<?> type, String name, DistScheme distScheme) {
        super(nodeID, nodes, type, name, distScheme);
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
